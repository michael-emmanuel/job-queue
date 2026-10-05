# Design notes: Milestone 2 (API and edge)

Decisions made while building the broker and HTTP layer, with the evidence
(tests) for each. They will be folded into `docs/tradeoffs.md`,
`docs/consistency.md` and `docs/failure-modes.md` later.

## 1. Layering

```
 client -> [outer: request id, trace id, panic recovery, metrics, access log]
        -> [route: authenticate -> authorise -> shed load -> deadline -> body limit]
        -> handler (validation, tenant/global rate limits)
        -> broker.Queue (routing, per-queue rate limit, fan-out)
        -> partition (state, WAL)
```

The API holds no queue logic and the partition knows nothing about HTTP.
Cheap rejections (auth, load shedding, size limits, rate limits) happen
before any partition lock is taken, so abusive traffic cannot slow
well-behaved tenants by contending for queue state.

## 2. Job IDs carry their route: `<queue>.<partition>.<random>`

Chosen: the ID encodes queue and partition, so ack/fail/renew/get need no
lookup table and are a single partition call.

Alternatives: (a) opaque IDs plus a job-to-partition index; (b) route every
worker call by queue name plus a separate lookup.

Why: an index is extra state to persist, replicate and keep consistent; an ID
that is its own address removes a whole class of metadata bugs.

Costs: IDs leak topology; changing a queue's partition count would orphan
existing IDs, which is one reason partition count is fixed at creation. In
a cluster, partition ownership moves between nodes, so the ID should name a
logical partition and let a routing table map it to a node (the virtual
shard design in `internal/routing`).

## 3. Routing key and hot partitions

Key priority: `partition_key`, else `idempotency_key`, else round-robin.
Using the idempotency key as a fallback is required for correctness: the
dedupe record lives in one partition, so a client retry must land on it.

Hashing balances keys, not load. `TestHotKeySkewIsNotFixedByHashing`: with
one merchant sending 80% of traffic, one of eight partitions receives at least
80% of jobs, however good the hash.

The consequence shows up in backpressure. `MaxDepth` is split evenly across
partitions, so `TestHotPartitionRejectsBeforeQueueIsFull` shows a queue with
a limit of 100 rejecting at 25 jobs when they all share a key: the other three
partitions sat empty. This is the honest behaviour of a partitioned system; a
queue-wide counter would hide the skew and let the hot partition's memory grow.
Mitigations (virtual shards, key salting, dedicated partitions, splitting)
are discussed in the hot-partition document; none is implemented yet.

## 4. Rate limiting versus backpressure

Both exist and answer different questions.

| | Rate limit | Backpressure |
|---|---|---|
| Question | how fast MAY this caller go | how fast CAN the system go now |
| Depends on | configuration | queue depth, load |
| Where | tenant, global (API), per-queue enqueue and consume (broker) | partition `MaxDepth`, in-flight semaphores |
| Response | 429 `rate_limited` + `Retry-After` | 429 `queue_full`, 503 `overloaded` |

Admission order is tenant, then global, then queue. A caller already over its
own limit is rejected without touching the shared budget. If a later check
rejects the request, earlier tokens are refunded
(`TestGlobalRateLimitDoesNotChargeRejectedTenants`,
`TestEnqueueRateLimitRefundsRejectedAndDuplicateRequests`), so rejected
requests do not drain budgets. Idle consumer polls refund their tokens too
(`TestEmptyPullRefundsConsumeTokens`), otherwise polling an empty queue would
starve the consume budget.

`Retry-After` is rounded up so a client is never told to retry too early.

Limitations: limits are per process (a cluster needs a shared or
approximately-divided budget); tenants share one default rate (no per-tenant
override yet); tenant depth and concurrent-worker quotas are not implemented.

## 5. What is bounded in memory

Request bodies (`MaxBodyBytes`, 413), per-job payload (queue config), jobs per
partition (`MaxDepth`), concurrent requests and concurrent long polls
(semaphores; excess gets 503 immediately instead of queueing), long-poll
duration, failure message length (2 KiB). Not yet bounded: the DLQ and the
idempotency index (both grow with traffic and TTL).

## 6. A timeout is not a failure

If a submit's context expires while waiting for fsync, the job is already
accepted. The API returns 504 whose message tells the client the effect is
unknown and to retry with the same idempotency key, and it does not refund
rate-limit tokens (the request may have succeeded). This is the reason the
idempotency key exists.

## 7. Long polling across partitions

A queue's consumers wait on any of its partitions. Each partition calls
`OnReady` when jobs become ready, which fires a queue-level broadcast channel.
A waiter takes the channel before scanning and waits on it only if the scan
was empty, so a job arriving between the scan and the wait cannot be missed
(a lost wakeup). Scans start at a rotating partition so none is favoured.

Rejected alternative: one goroutine per partition per poll. It can lease jobs
from several partitions at once and strand the extras until their visibility
timeouts lapse.

Costs: any wakeup wakes every waiter on the queue (thundering herd; each
rescans and most go back to sleep), and there is no consumer affinity. Consumer
groups (Milestone 3) assign partitions to workers, which fixes both.

## 8. Authentication, authorisation, tenancy

- Bearer API keys, stored as SHA-256 hashes, compared in constant time across
  all entries. Keys come from the environment or a file, never flags; error
  messages never contain key material.
- The tenant comes from the key, never the body. Unknown JSON fields are
  rejected, so a body cannot even attempt to claim one.
- Roles: producer (own tenant's jobs), worker (the delivery protocol), admin
  (everything). A foreign tenant's job is reported as 404, not 403, so IDs
  cannot be probed.
- The worker role is tenant-agnostic: workers consume every tenant's jobs of
  a queue. Per-tenant worker pools would need queue-per-tenant or tenant
  filtering at pull time, which is not implemented.
- The binary refuses to start without keys unless `-insecure-no-auth` is given,
  and logs a warning when it is.
- Metrics, probes and pprof are on a separate admin listener (default
  loopback). `/metrics` returns 404 on the public port.
- Audit events (`audit=true`) cover queue create/delete, job cancel, DLQ
  replay/delete, authentication failures and authorisation failures.
- Logs never contain payloads or credentials (`TestLogsNeverContainSecretsOrPayloads`).

Not implemented: key rotation or revocation without restart, brute-force
throttling of failed authentication (do this at a gateway or WAF), mTLS,
per-key rate limits, encryption of data at rest.

## 9. Observability

Metrics are written by hand in Prometheus text format to keep the repository
dependency-free; swap in the official client if you adopt this for real.
Queue and partition state is exposed by scrape-time collectors reading the
partition's own counters, not by duplicate counters that could drift.

Exposed: request count and latency by route, in-flight requests, rejections by
reason, pull outcomes, enqueue-to-first-delivery delay, jobs by state per
partition (so skew is visible), enqueue/lease/ack/retry/dead-letter/expiry
counters per partition, log bytes, compactions, goroutines, heap, GC cycles
and pause, CPU seconds, data-disk free bytes.

Labels use route names, not raw paths, and queue/partition values that are
bounded by configuration, to keep cardinality finite.

Gaps: no "age of oldest ready job" gauge (the best single consumer-lag
signal), no replica lag (no replicas yet), no trace spans (trace IDs are
accepted from `traceparent` and logged for correlation). `ReadMemStats` stops
the world briefly; fine at normal scrape intervals.

## 10. Graceful shutdown order

1. `BeginDrain`: readiness turns red, long polls return empty at once.
2. `http.Server.Shutdown` for both listeners: in-flight requests finish.
3. `Broker.Close`: stop partition loops, flush and fsync every log.

Closing the broker last means no request can race a closing log. Tested end
to end in `cmd/broker` (restart finds its jobs; shutdown with an open 25s long
poll completes in under 5s).

## 11. Queue lifecycle and metadata

`queues.json` is replaced atomically (temp, fsync, rename, fsync directory).
Create writes metadata before any partition files exist; delete removes
metadata before the data. A crash can therefore leave a registered queue with
no data or a data directory with no registration, never the dangerous inverse.
Create removes any unregistered directory of the same name first, so a deleted
queue's jobs cannot reappear (`TestCrashBeforeDirectoryCleanupDoesNotResurrectJobs`).

## 12. Verified on the real binary

Beyond the in-process tests, the binary was exercised with curl: the
quickstart lifecycle (`examples/quickstart.sh`), then 20 submissions, 3
leases and 1 ack followed by `kill -9`. After restart the queue held exactly
the same state (ready 17, in flight 2), an idempotent retry of an accepted
submission was deduplicated, and all surviving leases could be acked. This
was run on a single-core sandbox VM and says nothing about throughput.

## 13. Known limitations

- Single node: no replication, so disk loss loses data.
- Job status for completed jobs is unavailable (404).
- `PartitionKey` ordering is best-effort FIFO within a partition; retries and
  restarts can reorder.
- Priority is three weighted classes, not ten strict levels.
- No per-tenant depth or worker quotas, no per-tenant rate overrides.
- Unknown routes return JSON 404 for any method, so a wrong method on a real
  path also reports 404 rather than 405.
- Load testing has not been done; no throughput claims are made.
