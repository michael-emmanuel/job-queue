# Design notes: Milestone 1 (core engine)

These notes record decisions made while building the single-partition engine.
They will be folded into `docs/tradeoffs.md` and `docs/failure-modes.md`
in the documentation milestone. Every claim here is backed by a test.

## 1. Event sourcing: the log is the source of truth

Chosen: every state change is a record. A live operation validates, appends
the record to the log, then calls the same `apply*` function that recovery
uses. Apply functions take every timestamp and delay from the record, so
replay reproduces live behavior exactly.

Alternatives: (a) mutable per-job records in a key-value store (BoltDB,
Pebble); (b) in-memory state with periodic full snapshots.

Why: lease, ack and retry are tiny state transitions, so an append-only log
turns them into sequential writes. The record stream is also exactly what a
follower needs in the replication milestone. Tested by
`TestReplayEquivalenceRandomOps`.

Cost: memory holds all live jobs (no spill to disk); recovery time grows with
log length until compaction runs. Prefer a KV store when the live set exceeds
RAM or point lookups of old jobs dominate.

Note on the spec: the requested `JobStore` with Append/Get/Update/Delete is
realised as `storage.Log`. The partition owns the mutable view, the store only
needs ordered durable records, which keeps backends replaceable with a much
smaller interface.

## 2. Group commit: Append and Wait are separate

`Append` buffers a record and fixes its order under the partition lock.
The returned `WaitFunc` is called after unlocking. A single syncer goroutine
flushes and fsyncs outside the WAL mutex, so appenders keep buffering during
an fsync and one fsync covers many records. `TestGroupCommitSharesFsyncs`
shows 100 concurrent appends need far fewer than 50 fsyncs.

Cost: an operation is briefly visible to others before it is durable. Enqueue
tolerates this (a crash loses only an unacknowledged enqueue).

## 3. Durability policies

`SyncAlways` (default): acknowledged means fsynced. `SyncPeriodic`: lose up to
`SyncEvery` on crash. `SyncNone`: survives process crash, not power loss.
After any fsync error the WAL is permanently failed (no retry), because a
retried fsync after a failure can report success for pages the kernel already
dropped. The operator restarts and recovers from what is on disk.

## 4. Fencing and the lease-durability bug

The attempt number is the fencing token. It increments on every delivery and
Ack/Fail/Renew must quote it; a worker whose lease lapsed and was reassigned
holds a stale token and is rejected (`TestVisibilityTimeoutRedeliversAnd
FencesStaleWorker`).

A first version did not wait for lease records to be durable, reasoning that a
lost lease only causes redelivery. `TestInFlightJobsAreRedeliveredAfterLong
Outage` showed this was wrong: after a crash the job reverted to its old
attempt number, so the next delivery reused a token still held by a surviving
worker. Fencing tokens must be unique, so lease records are now durable before
delivery. Renew records are still not waited on, since losing one only
shortens a lease and cannot reuse a token.

Limitation: this guarantee holds under `SyncAlways` only. Fencing here
protects the queue's own state. It cannot stop a zombie worker from performing
external side effects; that requires the side-effect target to check the token
or be idempotent.

## 5. Late acks

If a lease has lapsed but the job has not been re-leased, an ack is accepted
(`TestLateAckAcceptedUntilRedelivered`). The work is done and discarding the
ack would only cause a duplicate. Once redelivered, the old token is rejected.
Renew is stricter: it fails after a lapse, so a worker learns it lost the job.

## 6. Scheduler: one min-heap per partition

Retry backoff, delayed jobs and visibility deadlines share one binary heap:
O(log n) insert and pop, O(1) peek, memory proportional to pending timers.
`Run` sleeps until the earliest deadline; correctness does not depend on it
because every operation first advances due timers.

Alternatives: a timing wheel (O(1), better for millions of timers of similar
duration, coarser accuracy, more code); time buckets (cheap, accuracy equals
bucket width); polling a sorted store (simple, wasteful).

Costs: renewals and cancellations leave stale heap entries that are skipped
when popped, so the heap can hold more entries than live timers. It is
in-memory only and rebuilt from the log on recovery; it is partition-local, so
it never becomes a global bottleneck.

## 7. Priority: weighted classes, not strict priority

Priorities 1-10 map to high (8-10), normal (4-7) and low (1-3) classes served
4:2:1 while all have work (`TestWeightedFairness...`, 40/20/10 of 70). Low
priority always makes progress, and a single non-empty class gets all capacity
(`TestSchedulerIsWorkConserving`).

Alternatives: strict priority (simple, starves low work under sustained high
load); one FIFO (no priority); a heap on priority (exact order, same starvation
problem, O(log n)).

Costs: within a class, order is FIFO, so priority 10 and 8 are equivalent; high
jobs wait behind other high jobs; the weights are a policy to tune.

## 8. Idempotency lives in the partition

The key is scoped by tenant and queue and stored with the partition, so it is
written in the same log record as the job: the check and the insert are atomic
under the partition lock, and the key survives crash and compaction. Expiry
uses an insertion-ordered list and an amortised O(1) sweep, no full scans.

Requires that retries of a request reach the same partition, so the router
(Milestone 3) must derive the partition key from the idempotency key when no
explicit key is given. A retry with the same key but a different payload returns
the original job and is not compared; a production system might fingerprint the
request and reject mismatches.

Memory: roughly 100 bytes per key held for the TTL. At 1,000 jobs/s and a 24h
TTL that is about 8-9 GB per node; sizing is covered in capacity planning.

An enqueue that returns an error from the durability wait may still have
created the job. Clients must retry with the same key.

## 9. Compaction

Compaction is a snapshot of live state, not a merge of segments. Under the
partition lock it copies state and calls `Rotate`; encoding and I/O happen
outside the lock. `WriteSnapshot` writes `<id>.seg.tmp`, fsyncs, renames over
the sealed segment, fsyncs the directory, then deletes older segments.
Replay starts at the newest segment that begins with the snapshot marker, so
a crash between rename and cleanup cannot resurrect old records
(`TestCrashBetweenSnapshotRenameAndCleanup`).

Observed in tests: 2,000 enqueue/lease/ack cycles shrink to under a tenth of
the log. Retained idempotency keys dominate what remains.

Cost: the snapshot rewrites all live state, so write amplification is
proportional to live data each time; automatic compaction triggers only when
the log passes a threshold and has doubled since the last pass.

## 10. Recovery semantics

- Torn tail of the newest segment is truncated; any other corruption fails
  startup with `ErrCorrupt` rather than silently dropping data. A mid-segment
  corruption in the newest segment is indistinguishable from a torn tail and is
  truncated; this is the same tradeoff Kafka makes.
- In-flight jobs keep their original deadline. Live workers may still ack until
  then; after a long outage they are redelivered immediately.
- Ready queues and timers are rebuilt from state; FIFO order comes from `Seq`.
  A retried job re-enters in original-Seq order after a restart, so FIFO is
  best-effort, not a guarantee.

## 11. Concurrency choices

One mutex per partition guards all state, held only for memory work and for
buffering a record. Channels are used only for signalling: `wake` (cap 1,
non-blocking send, so it cannot block or leak) and `avail`, a channel closed to
broadcast "work arrived" to long-pollers, which composes with `select`, timeouts
and contexts in a way `sync.Cond` cannot. Every goroutine has an owner: the
WAL syncer stops on `Close`/`Abort`, and `Run` stops on its context.

## 12. Known limitations

- Live and dead state is held in memory; the DLQ is not size-bounded and is
  not counted toward `MaxDepth`.
- Completed jobs are not retained, so job status for finished jobs is unknown
  (planned: a bounded recent-completion cache, explicitly non-durable).
- JSON record encoding is chosen for debuggability; a binary format would cut
  CPU and log size.
- Backpressure is per partition; per-queue and per-tenant quotas arrive with
  the API milestone.
- A single process, a single node: no replication, so a disk loss loses data.
- Timestamps come from the local wall clock; skew handling is not yet
  implemented.
