# Design notes: Milestone 4 (operations)

Turns the simulated cluster into real processes and adds the tooling to
break it on purpose.

- **HTTP transport** (`internal/cluster/httptransport.go`): the same `Transport`
  interface over HTTP/JSON with a shared-secret header; secret required unless
  `-insecure-no-cluster-auth`. A stale-heartbeat guard (`leaderSince`) in the
  coordinator stops an old heartbeat from revoking fresh leadership.
- **Fault injection**: `/faults` on the admin listener (only with
  `-enable-fault-injection`) blocks traffic by peer, in either direction.
  Combined with SIGKILL and SIGSTOP this gives crash, freeze and partition cases.
- **Binaries**: coordinator, node, gateway (cluster-mode public API through the
  `Backend` interface shared with the single-node broker), worker, scheduler,
  loadtest, jqctl.
- **Client library** (`pkg/client`): retry rules (429/503 always, unknown outcomes
  only with an idempotency key, bounded ambiguous retries), worker pool with
  lease renewal and graceful drain.
- **Scheduler**: recurring jobs; idempotency key derived from schedule name and
  time slot so two schedulers produce one job per slot.
- **Load generator**: open-loop (arrival schedule independent of response time)
  to avoid coordinated omission; reports achieved rate, latency, e2e, depth and
  process resources.
- **Durable counters**: logged counters are rebuilt on replay and carried through
  compaction by a snapshot meta record; unlogged ones (`rejected`, `duplicates`,
  `lease_expired`) are per-process.

## Known issues
- First election at startup can fail once ("no majority promised") because nodes
  start hosting replicas on their next heartbeat; it succeeds at epoch 2.
- Docker compose files in `deploy/` are written but untested: this sandbox has no
  Docker. The local process cluster (`scripts/cluster.sh`) is what was verified.
- The OpenAPI file is hand-written, not generated or contract-tested.
- A demo harness bug (jqctl exited 0 on a rejected single submit, so the demo over-counted accepted jobs) made net-partition fail about 1 run in 4 with 59 of 60. It was a counting bug in the tooling, not data loss; jqctl now exits non-zero.
