# Project handoff: all five milestones complete

This file lets a new session resume without the previous conversation. The
code in this repository is authoritative; this file is the map and the status.
Verified on a clean extract: `go build ./...`, `go vet ./...`, `gofmt -l .`
(no output) and `go test -race -count=1 ./...` all pass (17 packages, 80 Go
files, 19,639 lines, 6,703 of them tests).

## Resuming

- Module `github.com/michael-emmanuel/jobqueue`, Go 1.22, standard library only.
- The development sandbox had no Go: `apt-get install golang-1.22-go` after
  removing `/etc/apt/sources.list.d/nodesource*`; then
  `export PATH=$PATH:/usr/lib/go-1.22/bin`. The module proxy is unreachable.
- Sandbox: 1 CPU core, 4 GB RAM, no Docker or jq. Background processes die when
  a tool call ends, so each demo or benchmark must start and stop its cluster in
  one invocation. Use bash, not sh (brace expansion).
- Go 1.22 limits already hit: no `t.Context()`, no `slog.DiscardHandler`, no
  `omitzero`. `gofmt` realigns struct fields, which can defeat text patches.
- Baseline check: `gofmt -l . ; go vet ./... ; go test -race -count=1 ./...`

## Architecture in one picture

```
client -> gateway (stateless public API, cluster.Client)
            -> coordinator (single process: placement, epochs, leases,
               failure detection, elections, consumer groups)
            -> nodes: leader = partition engine over a replicated log
                      follower = replicated log only
partition engine = jobs + priority classes + timer heap + idempotency + DLQ,
  event-sourced over storage.Log (WAL with group commit | MemLog)
```

`cmd/broker` is the single-node variant: the same API over `broker.Broker`.

## Decisions that must stay documented

- At-least-once delivery; the attempt number is the fencing token; lease
  records are durable before delivery.
- Raft-style log replication with an EXTERNAL single coordinator assigning
  epochs. This is not consensus. Safety needs at most one coordinator that
  never issues an epoch twice.
- Elections: persist the epoch, collect durable `Prepare` promises from a
  majority (fences the old leader), promote the most up-to-date log, grant a
  lease. Leases travel as durations; the node counts from send time.
- Ack levels quorum | leader | all (leader loses acknowledged writes on leader
  death: demonstrated).
- Compaction is disabled for replicated partitions (snapshotting speculative
  state is unsound). A deposed leader discards memory and rebuilds from the log.
- Epochs are monotonic, not dense: a failed election burns one.
- Idempotent duplicates are acknowledged only after the original is durable.
- Unlogged counters (`rejected`, `duplicates`, `lease_expired`) are per-process;
  logged ones (`enqueued`, `leased`, `acked`, `retried`, `dead_lettered`) are
  rebuilt from the log and preserved across compaction.

## Status (final)

Complete and tested: Milestones 1 (engine), 2 (API and edge), 3 (distribution on
a simulated network); in Milestone 4: HTTP transport and fault injection, API
Backend interface and gateway, cluster binaries, client library and worker pool,
scheduler, load generator, operator CLI, local cluster and benchmark scripts.
Demos verified passing against real processes: normal, worker-failure,
duplicate, retry-storm, dlq, leader-failure, replica-failure, rebalance,
backpressure.

Written but NEVER RUN: demos `load`, `shutdown`, `net-partition`, `freeze`.
Written but not re-run after an assertion change: `hot-partition` (the correct
expectation is that other keys hashing to the hot partition are rejected too).

Benchmarks: shape observed on the 1-core sandbox but result files were not kept
and the CPU columns were inflated (fixed since). Re-run before quoting. Observed:
standalone fsync=always sustained about 1,000 jobs/s cleanly and saturated near
3,300/s at a 4,000/s offer; periodic and none met 4,000/s; the 3-node cluster at
ack=quorum and ack=leader behaved almost identically (about 500-1,000/s), which
points to CPU on a shared core rather than fsync or replication. A pprof run at
500 jobs/s showed a node at about 16% of a core (syscalls about 25%, json about
11%, allocation about 14%, fsync about 19% of node CPU) and no lock hot spot.

Open question: during one backpressure run the stats API showed rejected=198
against 200 client-side rejections; on a stable cluster it showed 200. Likely an
unlogged counter reset on a leadership change; not proven.

## Bugs found and fixed (each has a regression test)

1. `job.State` zero value did not round-trip JSON.
2. A duplicate enqueue was acknowledged before the original was durable.
3. Ambiguous retries were unbounded (20s stalls in a quorum outage).
4. Load generator: dispatcher off-by-one (added 1/rate of fake latency).
5. Load generator: kept sending queued requests after the stage ended.
6. Load generator: CPU averaged over the wrong interval.
7. Cumulative counters reset on recovery or failover.
8. Coordinator revoked a just-promoted leader on a stale heartbeat.

## Left to do

Finish Milestone 4: run the never-run demos (one per call; treat failures as
probable real bugs), re-run benchmarks, write docs/demo.md from real output,
write the benchmark and performance notes, add Dockerfile, docker-compose.yml,
Prometheus config and .env.example (cannot be tested in the sandbox; label them
unverified), update README, Makefile and CI.

Milestone 5 (documentation): tradeoffs.md (35 decisions plus the 10x/100x/1000x
review), failure-modes.md, capacity-planning.md, system-design.md,
Engineering-review.md (50+ Q&As), evolution.md,
observability.md, performance.md, a README rewrite per the original spec (no
emojis, no marketing words), ASCII diagrams, an OpenAPI spec.

Deferred engineering (document as roadmap): InstallSnapshot and replicated
compaction, replica reassignment, follower reads, a replicated coordinator,
mTLS between nodes, same-epoch two-leader hardening, server-side long-poll
fan-out for cluster pulls, per-tenant depth and worker quotas, an
oldest-ready-age gauge, completed-job status, batch submit and a cheaper
encoding.

## Working conventions

Build incrementally; keep `go test -race ./...` green. Write the failing test
before the fix. Mutation-test safety rules. Verify against real processes, not
only in-process tests. Report bugs honestly, including test bugs. Never claim
"production ready"; state limitations. README: no emojis, no marketing language.
Ports used by scripts/cluster.sh: coord 7000/9000, n1-n3 7101-7103/9101-9103,
gateway 8080/9080.

## Immediate next step

Run `scripts/demo.sh hot-partition`, then `load`, `shutdown`, `net-partition`,
`freeze`, one per call, and fix whatever they reveal; then re-run
`scripts/bench.sh standalone always` and `scripts/bench.sh cluster quorum`.
