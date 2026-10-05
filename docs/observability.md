# Observability

Admin listener per process (`-admin-listen`): `/metrics`, `/healthz`, `/readyz`,
`/debug/pprof` with `-enable-pprof`, `/faults` with `-enable-fault-injection`.
Keep the admin listener private. Logs are structured JSON with request ids.

## Metrics worth watching (names as exported)
| Question | Metrics |
|----------|---------|
| Are jobs flowing? | `jobqueue_enqueued_total`, `_leased_total`, `_acked_total`, `jobqueue_partition_jobs{state}` |
| Is anything failing? | `_retried_total`, `_dead_lettered_total`, `_lease_expired_total`, `jobqueue_worker_failed_total`, `_worker_lease_lost_total` |
| Are we slow? | `jobqueue_queue_delay_seconds` (histogram), API latency histograms |
| Overloaded? | `jobqueue_rejected_total` by reason, `jobqueue_pull_requests_total` |
| Replication healthy? | `jobqueue_replica_lag_entries`, `_replica_commit_index`, `_replica_durable_index`, `_replica_is_leader`, `_replica_epoch` |
| Cluster healthy? | `jobqueue_cluster_alive_nodes`, `_leaderless_partitions`, `_leader_elections_total`, `_failed_elections_total` |
| Groups | `jobqueue_group_members`, `_group_generation`, `_group_rebalances_total` |
| Workers | `jobqueue_worker_active_jobs`, `_worker_utilization`, `_worker_concurrency` |

Example alerts: deploy/prometheus/alerts.yml. Dashboard panels to build:
throughput by state, queue delay p50/p99, backlog per partition (skew),
leaders per node, replica lag, retry and DLQ rates.

## Logs
Elections, step-downs ("lease expired; stepping down", "deposed"), failed
elections, and audit events (submit, cancel, DLQ ops) are logged at info/warn
with partition and epoch. Never logged: payloads, API keys.

## Debugging recipes
- Backlog growing: `jqctl` queue stats; check worker utilisation and lease_expired.
- Duplicates: look at lease_expired and worker_lease_lost; handler slower than
  visibility timeout is the usual cause.
- Writes failing: leaderless partitions, then coordinator logs, then replica lag.
