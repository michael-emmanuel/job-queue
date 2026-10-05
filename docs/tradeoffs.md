# Design decisions and tradeoffs

Each entry: decision, why, what it costs, when to revisit. "Where" names the
code or test that shows it. Section 2 reviews the design at 10x, 100x and 1000x.

## 1. Decisions

1. **At-least-once delivery, not exactly-once.** Exactly-once needs the consumer's
   side effect and the ack to commit atomically, which a queue cannot arrange
   for arbitrary handlers. We give at-least-once plus tools (attempt fencing,
   idempotency keys) so handlers can be made effectively-once. Cost: duplicates
   after worker crash or slow ack. Revisit: if handlers write to a transactional
   store the queue can share.
2. **Attempt number as the fencing token.** Every delivery increments the
   attempt; ack/fail/renew must quote it. A superseded worker gets 409
   `stale_lease`. Cost: a late ack from a slow worker is rejected after
   redelivery. Cheaper and simpler than lease IDs.
3. **Late ack accepted until redelivery.** If the lease expired but nobody else
   has the job yet, the ack still counts. Avoids pointless duplicate work.
4. **Lease records are durable before delivery.** Otherwise a crash could reuse
   a fencing token (a bug we hit and fixed). Cost: one more log record per
   delivery, amortised by group commit.
5. **Event-sourced partition: every state change is a log record.** One apply
   function serves live operation and replay, so recovery cannot drift from
   live behaviour (a property test checks this). Cost: log growth, so snapshots.
6. **WAL with group commit.** Many appends share one fsync; each caller gets a
   WaitFunc. Throughput scales with concurrency, latency pays one fsync.
   fsync failure is sticky: after an error we stop accepting writes rather than
   guess what reached disk.
7. **CRC32C framing and torn-tail truncation.** A crash mid-write leaves a
   partial record; recovery drops it. Corruption in the middle is an error, not
   silently skipped.
8. **Snapshot compaction by atomic rename (single node only).** Bounds replay
   time. Disabled for replicated partitions because snapshotting speculative
   (uncommitted) state is unsound. Cost: replicated logs only grow in this
   implementation. Revisit: snapshot at the commit index.
9. **Hash partitioning FNV-1a(queue, key) mod N.** Same key, same partition,
   so per-key ordering is possible and hot keys are visible. Cost: no resizing
   without remapping, and a hot key makes a hot partition (demo 10).
10. **Partition count fixed at queue creation.** Simplicity over elasticity.
    Revisit: consistent hashing or split/merge.
11. **Per-partition depth limit = queue limit / N.** Enforcement stays local to
    one partition with no cross-partition coordination. Cost: a hot partition
    rejects while the queue is far from its limit (demo 10 shows it).
12. **Weighted fair priority classes (4:2:1), work-conserving.** Low priority is
    never starved yet idle capacity is not wasted. Cost: strict priority is not
    available; high priority gets about 4/7 of deliveries under contention.
13. **Timer min-heap for retries, schedules and visibility.** O(log n) and no
    scanning. Cost: memory proportional to timers; rebuilt on replay.
14. **Retries: exponential backoff with jitter.** Jitter prevents synchronized
    retry storms. Cost: unpredictable retry times, so tests inject a seeded RNG.
15. **Lease-exhaustion counts as an attempt.** A job that crashes workers (a
    poison job) reaches the DLQ instead of looping forever.
16. **DLQ inside the partition, with replay.** Keeps the failure record
    atomic with the state change. Cost: DLQ shares the partition's capacity.
17. **Idempotency scoped per tenant and queue, with TTL.** Tenants cannot
    collide; memory is bounded by TTL. Cost: after TTL a retry creates a
    duplicate. Duplicates are acknowledged only after the original is durable.
18. **Bounded everything.** Body size, payload size, in-flight requests, long
    polls, depth. Overload is a fast 429/503 with Retry-After, never an
    unbounded queue in memory.
19. **Token buckets for rate limits** (global, tenant, queue). Allow bursts,
    cap the average. Cost: per-process state; in a multi-gateway deployment
    the effective limit is per gateway.
20. **Long polling, not push.** Workers pull, so a slow worker cannot be
    overrun, and the server needs no worker registry. Cost: one held request
    per idle worker.
21. **Raft-style log replication with an EXTERNAL coordinator.** Epochs,
    log matching, truncation, quorum commit are implemented. Election is driven
    by a single process. Cost: not consensus; the coordinator is a single point
    of failure for control (not for the write path while leases last). Revisit:
    replace it with a Raft/etcd-backed coordinator.
22. **Quorum commit counts only current-epoch entries; leader appends a no-op.**
    The standard Raft rule that prevents committing an old entry that a later
    leader could overwrite.
23. **Persist the epoch before Prepare.** A restarted coordinator never reuses
    an epoch. Failed elections burn epochs (epochs are monotonic, not dense).
24. **Majority durable promises fence the old leader.** A follower that
    promises epoch e rejects older leaders, so the old leader cannot reach a
    quorum.
25. **Leases are granted as durations and counted from send time.** No
    cross-machine clock comparison; only the rate of clocks matters. Cost:
    a frozen or very-slow-clock leader can serve stale reads until its lease
    ends (tested), but cannot commit writes.
26. **Ack levels quorum | leader | all.** Honest knob: `leader` loses acked
    writes if the leader dies before replicating (demonstrated by test).
27. **Followers hold only a log.** Cheap replicas; a new leader replays.
    Cost: failover time grows with log length.
28. **Consumer groups: eager, central, sticky, generation-fenced.** Simple to
    reason about. Cost: eager rebalance pauses the whole group.
29. **Single-process coordinator.** Easiest correct thing. Documented loudly.
30. **Shared-secret cluster auth, bearer-key public auth.** Adequate for a
    teaching system; no mTLS or key rotation workflow. TLS is optional.
31. **HTTP/JSON between nodes.** Debuggable with curl; costs CPU and bytes
    compared with a binary protocol.
32. **Open-loop load generator.** Closed-loop generators hide latency under
    overload (coordinated omission). Cost: more complex harness.
33. **Client retry rules.** 429/503 always retried; unknown outcomes (timeouts)
    retried only when an idempotency key exists. Slot-derived keys make the
    scheduler safe to run on more than one instance.
34. **Standard library only.** No supply chain, builds offline. Cost: a
    hand-rolled metrics registry and router.
35. **No fabricated numbers.** Benchmarks state the environment; absent
    measurements are said to be absent.

## 2. Final review: what breaks at 10x, 100x, 1000x

Baseline: 3 nodes, one coordinator, partitions in the tens, JSON over HTTP.

**10x (thousands of jobs/s per queue).**
- Fsync and HTTP/JSON CPU dominate. More partitions spread both across leaders;
  batching pulls and enqueues cuts per-request overhead.
- Gateway rate limiters become per-gateway; tenants get N x their quota with N
  gateways. Needs a shared limiter or sticky routing.
- Hot partitions appear first. Mitigation is key design, not more hardware.

**100x.**
- Replicated logs never compact: disk and failover-replay time grow. Snapshots
  at the commit index become mandatory.
- Coordinator heartbeat and metadata fan-out grows with nodes x partitions. Its
  single-process design is now a real availability risk; move to a replicated
  control plane.
- Eager rebalance of large groups causes visible stalls; incremental
  (cooperative) rebalance is needed.
- Per-partition in-memory state (timers, idempotency index) bounds partitions
  per node by RAM.

**1000x.**
- A fixed partition count is a ceiling; need split/merge or consistent
  hashing, plus rack-aware placement.
- JSON replication wastes network; a binary protocol with batching and
  compression is needed. Cross-region needs async replication and a different
  consistency story.
- Observability cardinality (per partition metrics) must be sampled.
- At this scale you should be buying or adopting Kafka/SQS/Pulsar unless you
  have a specific reason not to. This project is a design exercise.
