# Demos

Every demo is `scripts/demo.sh <name>`; it builds, starts a local cluster of
real processes, injects the failure, asserts the outcome and tears down.
All 12 passed in the final session (Go 1.24.7, 2 CPUs).
Run all with `scripts/demo.sh all`.

| # | Name | Failure injected | What it proves |
|---|------|------------------|----------------|
| 1 | normal | none | submit, pull, ack end to end |
| 2 | worker-failure | worker killed mid-job | lease expiry, redelivery with attempt+1 |
| 3 | duplicate | producer retries | idempotency key returns original |
| 4 | retry-storm | handlers fail en masse | backoff and jitter spread second attempts |
| 5 | dlq | poison job | DLQ after max attempts, replay |
| 6 | leader-failure | kill -9 a leader | election, no acked loss, continued service |
| 7 | replica-failure | kill a follower | writes continue, catch-up on return |
| 8 | rebalance | worker joins/dies | group rebalance, generation fencing |
| 9 | backpressure | flood | 429/503 with Retry-After, bounded depth |
| 10 | hot-partition | one hot key | partition-local rejection; blast radius |
| 11 | load | open-loop stages | the table in performance.md |
| 12 | shutdown | SIGTERM worker, gateway, node | graceful drain, restart intact |
| extra | net-partition | isolate leader both ways | lease step-down, no split brain |
| extra | freeze | SIGSTOP leader 9 s | deposed on thaw, exactly-once processing |

Manual play: `scripts/cluster.sh up`, `source <(scripts/cluster.sh env)`,
then curl or `bin/jqctl`. Failure injection: `scripts/cluster.sh kill n1`,
`pause`/`resume`, `faults n1 '{"block_to":["n2"]}'`.
