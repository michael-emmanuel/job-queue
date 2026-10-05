# System design

## Requirements
Functional: submit jobs (delayed, prioritised, idempotent), deliver to workers
with visibility timeouts, retry with backoff, dead-letter, replay.
Non-functional: no acknowledged-job loss under single-node failure (ack=quorum),
bounded memory and latency under overload, observable, testable failure.
Non-goals: exactly-once, global ordering, multi-region, consensus.

## Components
```
 producers --> gateway (stateless: auth, tenant, rate limit, shedding)
                  |  metadata cache, leader routing
                  v
 coordinator (single process) --- heartbeats/leases/elections ---> nodes n1..nN
                                                                   leader: partition engine over replicated log
                                                                   follower: log only
 workers <-- long-poll pull via gateway (consumer group assigns partitions)
 scheduler --> submits recurring jobs with slot-derived idempotency keys
```

## Write path
1. Gateway authenticates, applies limits, hashes (queue, partition_key) to a partition.
2. Request goes to that partition's leader (cached metadata; refreshed on error).
3. Leader checks idempotency and depth, appends the enqueue record to its log.
4. Record replicates to followers; commit when a majority has fsynced.
5. Leader applies it, answers 201. Ack level may relax step 4 (leader) or tighten it (all).

## Delivery path
Pull -> leader picks a job by weighted priority class -> appends a lease record
(attempt+1, deadline) -> waits for durability -> returns the job. Ack, fail and
renew quote the attempt. The timer heap fires visibility expiry, retry release
and scheduled release; each is itself a log record.

## State machine of a job
```
 scheduled --run_at--> ready --pull--> in_flight --ack--> (done)
                         ^                |  \--fail/expiry--> retry_wait --backoff--> ready
                         |                |                \--attempts exhausted--> dead (DLQ)
                         +----replay------+
```

## Why a log
One ordered record stream gives crash recovery, replication and tests from one
mechanism. Apply is deterministic; the same function runs live and on replay.

## Honest limits
Coordinator is a single process and not consensus (see consistency.md).
Replicated logs do not compact. JSON everywhere. See tradeoffs.md.
