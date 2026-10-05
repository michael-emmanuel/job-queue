# Performance

## Read this first
Everything here was measured in one session on a shared cloud sandbox:
Intel Xeon @ 2.10GHz, 2 CPUs, Go 1.24.7, linux/amd64, loopback network, a
single machine running every process (3 nodes, coordinator, gateway, load
generator). Each number is ONE run, not a distribution. The sandbox gives no
control over noisy neighbours and unknown disk characteristics. These figures
show shape and order of magnitude; they are not a capacity claim and must not
be compared with other systems.

## Microbenchmarks (go test -bench)
| Benchmark | Result |
|-----------|--------|
| enqueue+dequeue+ack, MemLog (no fsync) | 6.4 microseconds per cycle |
| WAL append, SyncNone, 256 B record | 402 ns per record |

Both exclude fsync. Durable throughput depends on the disk and is not measured here.

## Cluster load test (scripts/demo.sh load; open-loop, 256 B payload, idempotency keys on, 3 nodes, 4 partitions, 6 s per stage)
| target/s | achieved | submit p50 | p95 | p99 | e2e p50 | e2e p99 | max depth |
|---------:|---------:|-----------:|----:|----:|--------:|--------:|----------:|
| 100 | 100 | 3.1 ms | 7.2 ms | 153.9 ms | 16.3 ms | 170.0 ms | 6 |
| 500 | 500 | 9.2 ms | 21.2 ms | 28.5 ms | 28.3 ms | 72.4 ms | 16 |
| 1000 | 1000 | 18.9 ms | 45.3 ms | 66.8 ms | 66.2 ms | 174.7 ms | 86 |

Observations, with the confidence each deserves:
- The system kept up through 1000 jobs/s with no 429s. We did NOT push to
  saturation, so this says nothing about the maximum.
- Latency grows with load while CPU is shared by all processes on 2 cores
  (each of the three nodes used roughly 0.3 core at 1000/s). Co-location makes
  latency here pessimistic and throughput ceilings unrepresentative.
- The 100/s stage had 2 server errors and a 154 ms p99, a first-stage effect
  (cold connections and, probably, the startup election window). Not confirmed.
- Stage lengths of 6 s are short; no soak, no GC-pause analysis.

## Not done
- No pprof-driven optimisation was performed in the final state; any earlier
  profiling findings are not re-validated and are not claimed.
- No fsync-bound benchmark, no multi-machine test, no large-payload test.

## Reproduce
`scripts/demo.sh load` (starts and stops its own cluster), or
`scripts/bench.sh` and `cmd/loadtest -h` for custom rates.
