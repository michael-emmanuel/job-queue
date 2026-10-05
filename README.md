# Distributed Job Queue

A distributed job queue in Go, built to make the engineering tradeoffs of
systems like SQS, Kafka consumer groups and Sidekiq explicit and testable:
at-least-once delivery, visibility timeouts with fencing, retries with
jittered exponential backoff, dead-letter queues, scheduling, idempotency,
backpressure, partitioning and replication.

## Status

This repository is built in milestones. Each one compiles, passes
`go test -race ./...`, and documents only what the code does.

| Milestone        | Scope                                                                                                                                                                                                                                              | State                      |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------- |
| 1. Core engine   | Job model, durable WAL, partition engine (visibility timeout, retries, DLQ, scheduling, priority, idempotency, backpressure, compaction, crash recovery)                                                                                           | Done                       |
| 2. API and edge  | HTTP API, auth and tenancy, rate limiting, backpressure, Prometheus metrics, structured logging, graceful shutdown, single-node `broker` binary                                                                                                    | Done                       |
| 3. Distribution  | Replication with quorum commit, coordinator-driven election with epochs and leases, failure detection, consumer groups, client routing. Tested on a fault-injecting in-memory network; not yet reachable over HTTP                                 | Done (simulated transport) |
| 4. Operations    | HTTP transport, fault injection, cluster binaries (coordinator, node, gateway), client library and worker pool, scheduler, load generator, operator CLI, local cluster and demo scripts, docker compose (untested, no Docker in the build sandbox) | Done                       |
| 5. Documentation | Tradeoffs, failure modes, capacity planning, Engineering Review, demos                                                                                                                                                                             | Done; see docs/            |

Nothing in this repository is claimed to be production ready.

## Quick start

```
make build
JOBQUEUE_API_KEYS="admin-key-0123456789:ops:admin,prod-key-01234567890:acme:producer,work-key-012345678901:workers:worker" \
  ./bin/broker -data-dir ./data -log-json=false
./examples/quickstart.sh        # in another terminal
```

`make run` does the first two steps with the development keys above. Those
keys are for local use only. The server refuses to start without keys unless
`-insecure-no-auth` is passed.

Key format: `key:tenant:role+role,...` with roles `producer`, `worker`, `admin`.
Keys come from `JOBQUEUE_API_KEYS` or `JOBQUEUE_API_KEYS_FILE`, never from flags.

## API

Submit (producer). Retrying with the same key returns the original job.

```
POST /v1/jobs
Idempotency-Key: signup-123
{"queue":"email","type":"send_email","payload":{"user_id":"123"},"priority":5,
 "run_at":"2026-10-02T15:00:00Z"}
-> 201 {"job_id":"email.2.000001a0...","queue":"email","state":"ready","duplicate":false}
```

A worker's delivery:

```
POST /v1/queues/email/pull   {"worker":"worker-1","max":10,"visibility":"30s","wait":"20s"}
-> {"jobs":[{"job_id":"...","queue":"email","type":"send_email","payload":{...},
             "attempt":2,"visibility_deadline":"...","created_at":"..."}]}
```

| Endpoint                                                                    | Role             | Notes                                                          |
| --------------------------------------------------------------------------- | ---------------- | -------------------------------------------------------------- |
| `POST /v1/jobs`                                                             | producer         | tenant rate limit, global rate limit, queue limits             |
| `GET /v1/jobs/{id}`                                                         | producer, worker | producers see only their tenant's jobs; 404 for completed jobs |
| `DELETE /v1/jobs/{id}`                                                      | producer         | cancel a live job                                              |
| `POST /v1/queues/{q}/pull`                                                  | worker           | long poll, consume rate limit                                  |
| `POST /v1/jobs/{id}/ack`                                                    | worker           | `{"attempt":n}`; 409 `stale_lease` if superseded               |
| `POST /v1/jobs/{id}/fail`                                                   | worker           | `{"attempt":n,"error":"...","retryable":true}`                 |
| `POST /v1/jobs/{id}/renew`                                                  | worker           | heartbeat; extends the visibility deadline                     |
| `POST/GET /v1/queues`, `GET/DELETE /v1/queues/{q}`                          | admin            | queue lifecycle and statistics                                 |
| `GET /v1/queues/{q}/dlq`, `POST .../dlq/{id}/replay`, `DELETE .../dlq/{id}` | admin            | dead-letter queue                                              |
| `GET /healthz`, `GET /readyz`                                               | none             | liveness and readiness                                         |

Errors share one shape, `{"error":{"code","message","request_id"}}`. Overload
responses carry `Retry-After`: 429 `rate_limited` and `queue_full`, 503 `overloaded`.

Admin listener (default `127.0.0.1:9090`): `/metrics`, `/healthz`, `/readyz`,
and `/debug/pprof` with `-enable-pprof`.

## What exists today

```
 producer                                         worker
    | POST /v1/jobs                    POST pull   ^
    v                                              |
+------------------------------ API ---------------------------------+
| auth, roles, tenant | strict JSON | rate limits | load shedding     |
| request/trace IDs   | metrics     | audit log   | graceful drain    |
+------------------------------+-------------------------------------+
                               v
+------------------------------ Broker -------------------------------+
| queues.json (atomic)  router: hash(queue, key) % partitions         |
| long-poll fan-out across partitions   per-queue rate limits         |
+--------+------------------+------------------+----------------------+
         v                  v                  v
   Partition 0         Partition 1  ...   Partition N-1
   jobs, ready queues, timer heap, idempotency index, DLQ
         | append-then-apply (write-ahead)
         v
   storage.Log: WAL (CRC32C, segments, group-commit fsync,
                     torn-tail repair, snapshot compaction) | MemLog
```

Implemented and tested:

- At-least-once delivery with a per-delivery fencing token (the attempt number).
- Visibility timeout, lease renewal, late-ack acceptance until redelivery.
- Retry policy: exponential backoff, cap, equal or full jitter, retryable and
  non-retryable failures, poison-job detection through lease exhaustion.
- Dead-letter queue with metadata, replay and delete.
- Scheduled jobs and retry delays on a min-heap (no scanning).
- Weighted fair priority classes (no starvation, work-conserving).
- Idempotency keys scoped per tenant and queue, with TTL, surviving restart
  and compaction.
- Hash partitioning; job IDs encode their queue and partition.
- Backpressure: bounded partition backlog, bounded body and payload, bounded
  concurrent requests and long polls.
- Token-bucket limits: global, per tenant, per queue (enqueue and consume).
- Bearer-key authentication, roles, tenant isolation, audit log, TLS option.
- Prometheus metrics, structured JSON logs with request and trace IDs.
- Crash recovery, segment rotation, crash-safe snapshot compaction.
- Graceful shutdown: drain, stop servers, then flush logs.

`cmd/broker` is the single-node server. The cluster layer (below) runs as real
processes via `scripts/cluster.sh` and as an in-process simulation in tests.

## Distribution (Milestone 3)

```
  Coordinator: placement, epochs, leases, failure detector, consumer groups
        ^ heartbeats / metadata                       (single process)
        |
  Node n1            Node n2            Node n3
  q/0 LEADER ------> q/0 follower       q/0 follower      AppendEntries,
  q/1 follower       q/1 LEADER ------> q/1 follower      quorum commit
        ^
        | enqueue / pull / ack go to the partition leader (Client routes)
```

- Each partition has a replica set (default 3). Writes commit when a majority
  has fsynced them; `ack` can be `quorum`, `leader` or `all` per queue.
- The leader is chosen by a coordinator that persists a new epoch, collects
  durable promises from a majority (which fences the old leader), promotes the
  most up-to-date replica, and grants a time-bounded lease.
- Consumer groups assign partitions to workers (balanced, sticky), rebalance
  on join, leave or death, and fence stale members with generations.
- **This is not a consensus implementation.** It is Raft-style log replication
  with an external, single-process election authority. Safety depends on that
  authority never issuing an epoch twice. Read docs/design-notes-m3.md.

Demonstrated by tests (see the notes for each): committed writes survive
random leader failures; an isolated leader cannot acknowledge a write and its
uncommitted entries are discarded; a leader with a frozen clock serves stale
reads but cannot write; `ack=leader` loses acknowledged writes while
`ack=quorum` does not; losing a majority halts writes and elections; the
coordinator outage stops leaders after their lease; lagging and restarted
replicas catch up; dead workers' partitions and jobs are reassigned.

## Testing

```
make race     # go test -race -count=1 ./...
make cover
make bench
```

The suite includes unit tests, concurrency tests under the race detector,
fault-injection tests (`WAL.Abort` simulates a process kill; fsync failure is
injected), and a randomized property test asserting that state recovered from
the log equals the live state across arbitrary operation sequences,
including compactions.

Measured on a single-core Xeon 2.10GHz VM, ext4, Go 1.22.2 (not a
representative benchmark machine):

| Benchmark                               | Result                           |
| --------------------------------------- | -------------------------------- |
| enqueue + dequeue + ack on `MemLog`     | about 7.2 microseconds per cycle |
| WAL append, `SyncNone`, 256 byte record | about 413 ns per record          |

These exclude fsync latency; durable throughput depends on the disk.

## Layout

```
internal/clock       injectable clock (fake clock for deterministic tests)
internal/job         domain model: Job, states, DeadLetter
internal/retry       backoff and jitter policy
internal/storage     Log interface, WAL, MemLog
internal/partition   the queue engine
internal/routing     key to partition mapping
internal/ratelimit   token buckets
internal/broker      queues, persistence, routing, long-poll fan-out (single node)
internal/replication replicated log, epochs, quorum commit, fencing
internal/cluster     coordinator, nodes, client, consumer groups, test network
internal/api         HTTP edge
internal/observability  metrics registry, logging helpers
internal/config      flags and environment
cmd/broker           single-node server binary
examples/            quickstart script
docs/                design notes (more to come)
```

## Documentation

- [docs/design-notes-m1.md](docs/design-notes-m1.md): the storage and partition
  engine: decisions, alternatives, tradeoffs and known limitations.
- [docs/design-notes-m3.md](docs/design-notes-m3.md): replication, election,
  split brain, leases, fencing, consumer groups, limitations.
- [docs/consistency.md](docs/consistency.md): what is and is not guaranteed.
- [docs/HANDOFF.md](docs/HANDOFF.md): current state, open items and next steps.
- [docs/design-notes-m2.md](docs/design-notes-m2.md): the broker and HTTP edge:
  routing, hot partitions, rate limiting versus backpressure, security,
  observability, shutdown.

## Requirements

Go 1.22 or newer. The standard library only (the sandbox this was built in
cannot reach the module proxy, and the project does not need it).

## Run the cluster and the demos

```
scripts/cluster.sh up            # coordinator, 3 nodes, gateway (local processes)
source <(scripts/cluster.sh env)
scripts/demo.sh all              # 12 demos plus network-partition and freeze
scripts/cluster.sh down
```

Docker: `make compose-up` (untested here; see docs/design-notes-m4.md).

## Documentation map

| Doc                         | Content                                                     |
| --------------------------- | ----------------------------------------------------------- |
| docs/system-design.md       | requirements, components, write and delivery paths          |
| docs/tradeoffs.md           | 35 decisions and the 10x/100x/1000x review                  |
| docs/consistency.md         | what is and is not guaranteed                               |
| docs/failure-modes.md       | failure table with verification status                      |
| docs/capacity-planning.md   | sizing formulas                                             |
| docs/observability.md       | metrics, logs, alerts                                       |
| docs/performance.md         | measured numbers with environment, and what is not measured |
| docs/demo.md                | demo catalogue                                              |
| docs/evolution.md           | V1 to V9                                                    |
| docs/engineering-review.md  | 55 Q&As                                                     |
| docs/design-notes-m1..m4.md | per-milestone design notes                                  |
| api/openapi.yaml            | API description                                             |

## Limits, stated plainly

Not consensus (single external coordinator); replicated logs do not compact;
no mTLS or key rotation; benchmarks are single runs on a shared 2-CPU VM; no
Jepsen-style verification. Not production ready.
