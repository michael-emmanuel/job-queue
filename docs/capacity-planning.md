# Capacity planning

Formulas are reasoning aids. The only measurements are those in performance.md,
taken on a small shared VM; replace every constant with a measurement from your
own hardware before sizing anything.

## Inputs
- R: peak enqueue rate (jobs/s); S: average record size (bytes) including payload
- A: attempts per job (1 + retries), D: retention of unacked jobs (seconds)
- N: partitions, F: replication factor (3), W: workers, T: mean handler time (s)

## Formulas
- Workers needed: W >= R x A x T / utilisation (use 0.6 to 0.7). A job needing
  2 s at 100 jobs/s with 1.1 attempts needs about 220 / 0.65 = ~340 concurrent slots.
- Log writes per job: enqueue + lease + ack = at least 3 records (plus retries).
  Disk write rate per replica ~ R x A x 3 x (S + ~40 overhead).
- Network: leader sends each record to F-1 followers: ~ R x A x 3 x S x (F-1).
- Memory per partition ~ live jobs x (job size + index overhead) + timers +
  idempotency entries (TTL x R / N). Measure bytes/job with heap profile.
- Partitions: at least (nodes x cores) so every core has a leader to run, more if
  one partition's single-writer limit is the bottleneck. A hot key caps that
  partition at single-partition throughput regardless of N.
- Backlog drain time = backlog / (service rate - arrival rate). If service rate
  <= arrival rate the backlog never drains; that is the alert to have.
- Depth limit: backlog you are willing to wait out = limit / service rate.
- Failover: replay time ~ log length / replay rate; keep logs short or snapshot.

## Worked example (illustrative)
R = 1,000/s, S = 512 B, A = 1.05, F = 3: ~3.15k records/s x 550 B = ~1.7 MB/s
per replica, ~3.5 MB/s replication egress at the leader set, ~150 GB/day of log
per replica without compaction. That last figure is why the missing replicated
snapshots matter.

## What to monitor to know you are near the edge
queue delay p99, `jobqueue_replica_lag_entries`, fsync latency, goroutine count
on the gateway, partition skew (max/mean enqueued), worker utilisation.
