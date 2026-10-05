# Failure modes

"Verified" names how it was checked here: a Go test, a demo script (scripts/demo.sh)
or both. "Not verified" means reasoned only. Network is a simulated network in
unit tests and real processes with /faults, SIGKILL and SIGSTOP in demos.

| # | Failure | What happens | Data safety | Verified |
|---|---------|--------------|-------------|----------|
| 1 | Worker crashes mid-job | Lease expires; job redelivered with attempt+1 | At-least-once; handler must be idempotent | demo 2, tests |
| 2 | Worker is slow, not dead | Duplicate delivery; old worker's ack gets 409 `stale_lease` (unless the job has not been redelivered yet) | Fencing prevents double-completion | tests |
| 3 | Worker SIGTERM | Finishes in-flight within grace, acks, exits 0 | None lost | demo 12 |
| 4 | Producer retries after timeout | Same idempotency key returns original; no key means a duplicate | Duplicate only without key | demo 3, tests |
| 5 | Poison job | Fails or crashes repeatedly; reaches DLQ after max attempts | Preserved in DLQ | demo 5 |
| 6 | Retry storm | Backoff plus jitter spreads retries | n/a | demo 4 |
| 7 | Broker process crash (single node) | Replay from WAL; torn tail truncated | Acked-and-fsynced writes survive | tests (WAL.Abort) |
| 8 | fsync error | Log goes sticky-failed; writes refused; health fails | No acks after unknown disk state | tests |
| 9 | Disk full / corruption mid-log | Recovery refuses rather than skipping | Operator decision | tests (corruption) |
| 10 | Leader node dies | Coordinator detects, majority promises, promotes the most up-to-date replica, no-op | Quorum-acked writes survive | demo 6, tests |
| 11 | Leader dies with ack=leader | Unreplicated acked writes lost | Loss is possible by definition | test |
| 12 | Follower dies | Writes continue with a majority; follower catches up on return | None lost | demo 7, tests |
| 13 | Majority lost | Writes and elections halt; reads of committed state may stall | Safe, unavailable | test |
| 14 | Leader partitioned from cluster | Lease expires, it steps down; majority side elects; its uncommitted entries are discarded on heal | No split brain observed | net-partition demo, tests |
| 15 | Leader frozen (SIGSTOP / long GC) | Same as 14; on thaw it learns it was deposed | No split brain observed | freeze demo |
| 16 | Leader clock stops | Stale reads possible until lease ends; cannot commit | Writes safe; reads can be stale | test |
| 17 | Coordinator dies | Existing leaders keep working until leases expire, then stop; no failovers happen | Safe, degraded then unavailable | test |
| 18 | Coordinator restarts | Epoch persisted so none is reused | Safe | test |
| 19 | Two coordinators run at once | NOT SAFE. Epoch uniqueness is the safety assumption | Can split brain | Not verified; documented limit |
| 20 | Gateway dies | Stateless; clients retry elsewhere | None lost | demo 12 |
| 21 | Consumer group member dies | Session timeout, rebalance, generation bumps; stale members fenced | At-least-once | demo 8, tests |
| 22 | Overload | 429/503 with Retry-After; bounded queues | No loss; producers back off | demo 9 |
| 23 | Hot partition | That partition rejects while others are idle | No loss | demo 10 |
| 24 | Clock jump on a node | Timers use monotonic or injected clocks in the engine; wall-clock jump can affect `run_at` | Scheduling only | Not verified under real jumps |
| 25 | Byzantine node or bad actor on cluster network | Out of scope; shared secret only | n/a | Not addressed |
| 26 | Rolling upgrade across incompatible log formats | No version negotiation | n/a | Not addressed |

Known rough edge: at cluster startup the first election can fail ("no majority
promised") because nodes have not yet started hosting replicas; the coordinator
retries and succeeds with epoch 2. Harmless, visible in logs, and the load demo
saw 2 5xx responses in its first stage that are probably from this window
(unconfirmed).
