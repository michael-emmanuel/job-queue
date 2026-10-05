# Consistency model

What the system guarantees today, and what it explicitly does not. Anything
not listed as guaranteed should be assumed not guaranteed. Test names are
given where a guarantee is demonstrated.

## Summary

| Operation | Guarantee |
|---|---|
| Enqueue (acknowledged) | Durable per the queue's ack level. At `quorum`, survives the loss of any minority of replicas. |
| Enqueue (error or timeout) | **Unknown**: the job may or may not exist. Retry with an idempotency key. |
| Duplicate enqueue | Suppressed by idempotency key within its TTL, per tenant and queue. |
| Delivery | **At-least-once.** A job can be delivered more than once. |
| Ack (acknowledged) | Durable. The job will not be redelivered after a crash or failover. |
| Stale ack | Rejected (fencing token) once the job is redelivered. |
| Job status read | Served by the partition leader from memory. May include effects of writes not yet committed, and may be stale if the leader was deposed and its clock is wrong. |
| Ordering | Best effort FIFO within a partition and priority class. Not guaranteed across retries, failovers or restarts. |
| Replica reads | Not provided. |
| Consumer assignment | Eventually consistent; a stale member is fenced within about one heartbeat. |

## Enqueue

- With `ack=quorum` (default) an acknowledged enqueue has been fsynced by a
  majority of the partition's replicas. Any later leader holds it
  (`TestAcknowledgedWritesNeverLostUnderRandomFailovers`).
- With `ack=leader` an acknowledged enqueue may be lost if the leader fails
  before replicating; demonstrated: 5 of 5 acknowledged writes lost
  (`TestAckLevelTradesDurabilityForLatency`).
- With `ack=all` every replica must hold it; one dead replica blocks writes.
- Single-node broker: durability follows the WAL sync policy
  (`always`: acknowledged means fsynced; `periodic`: up to the interval can be
  lost; `none`: survives a process crash but not power loss).
- An error does not mean the job was not created. Examples: a timeout while
  waiting for durability; leadership lost mid-write
  (`ErrLeadershipLost`); a quorum that returns after the client gave up
  (`TestLosingAMajority...`). A client cannot distinguish these from failures
  that did not apply, which is why the idempotency key exists: retrying with
  the same key returns the original job.

## Visibility and leases

- A job leased to a worker is invisible to others until its visibility
  deadline or until it is acked or failed.
- Lease records are durable before the job is delivered. Otherwise a crash
  could reuse an attempt number that a surviving worker still holds, defeating
  fencing (found by test; see design-notes-m1).
- The attempt number is the fencing token. After a lease lapses and the job
  is leased again, the old holder's ack, fail and renew are rejected
  (`TestVisibilityTimeoutRedeliversAndFencesStaleWorker`).
- If a lease lapses and nobody has re-leased the job, the original worker's
  late ack is accepted.
- **Exactly-once is not provided.** A worker that performs an external side
  effect and crashes before acking causes redelivery. Handlers must be
  idempotent. Fencing protects the queue's state only; it cannot stop a zombie
  worker from performing a side effect.

## Acknowledgement

An acknowledged ack is durable at the queue's ack level. An ack that errors
may or may not have been applied; retrying returns `stale_lease` or
`job_not_found` if it was, which a worker should treat as success.

## Job status

`GET` goes to the leader and reads in-memory state. Consequences:

- It reflects every write the leader has accepted, **including writes not yet
  committed**. If the leader is deposed before committing them they disappear.
- A deposed leader keeps answering until its lease runs out. If its clock runs
  slowly or pauses, it can answer for much longer
  (`TestFrozenClockLeaderServesStaleReadsButCannotWrite` shows it reporting a
  job as `ready` after the real leader completed it).
- Completed jobs are not retained, so status for a finished job is "not found".

This is **not** linearizable reads. Making them so would require the leader to
confirm leadership with a quorum (a read index) before answering, which is not
implemented.

## Ordering

Within one partition, jobs of the same priority class are dequeued in
enqueue order, and jobs that share a partition key share a partition. This is
not a guarantee: retries re-enter at the tail, recovery rebuilds order from
sequence numbers, failover replays the log, and several workers hold several
jobs at once. Applications that need strict per-key ordering must serialise
processing per key themselves.

## Consumer groups

Assignments are computed centrally and distributed by heartbeat, so different
nodes may briefly disagree about the current generation. Pulls with an older
generation than a node knows are rejected. A member may overlap with its
successor on a partition for up to one heartbeat; job leases keep the overlap
safe.

## Partition and failure behaviour

| Situation | Behaviour |
|---|---|
| One replica down | Writes continue (quorum of the other two) |
| Leader down | Unavailable for about failure timeout + lease, then writes resume on a new leader |
| Majority of replicas down | No writes, no election; reads from the leader may continue until its lease ends |
| Coordinator down | Data plane works until leases expire, then writes stop; no failover |
| Leader network-isolated | It cannot acknowledge writes; stale reads possible until its lease ends |
| Duplicated or lost messages | Safe; duplicates are idempotent, loss is retried |

In CAP terms a partition with a minority of replicas reachable is **unavailable
for writes** rather than inconsistent: the system chooses consistency.
