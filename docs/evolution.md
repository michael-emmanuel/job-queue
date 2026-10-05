# Evolution: V1 to V9

Each version is a real stopping point with a stated limit. Only V1 through V4
are implemented here; V5 onward are design sketches.

| Version | What it is | Limit that motivates the next step |
|---------|-----------|------------------------------------|
| V1 | Single process, in-memory queue, a mutex | Lose everything on restart |
| V2 | Add WAL, visibility timeout, retries, DLQ (Milestone 1) | One machine, one disk |
| V3 | HTTP API, auth, limits, metrics, single-node broker (M2) | Node loss means outage |
| V4 | Partitions, replication, coordinator elections, groups, gateway, workers (M3, M4) | Coordinator SPOF; no replicated compaction; eager rebalance |
| V5 | Replace coordinator with a consensus-backed control plane (embedded Raft or etcd) | Fixed partition count |
| V6 | Snapshots at commit index; follower catch-up by snapshot | Eager rebalance stalls; JSON overhead |
| V7 | Cooperative rebalance; binary protocol with batching; shared distributed rate limiter | Single region |
| V8 | Partition split/merge, rack-aware placement, tiered storage for old segments | Cross-region |
| V9 | Multi-region: async replication, per-region queues, explicit conflict story, DR runbooks | Operational maturity, security review, multi-year support |

Beyond V4 the honest advice is to evaluate adopting an existing system.
