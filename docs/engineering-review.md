# Engineering Review

## A. Delivery semantics

1. **Why at-least-once?** Exactly-once needs atomic commit of side effect and ack across systems; the queue cannot do that for arbitrary handlers. We expose fencing and idempotency instead.
2. **How can a job run twice?** Worker crashes after the effect but before ack, or is slower than the visibility timeout. Test: worker-failure demo.
3. **What is a visibility timeout?** A lease; if not acked or renewed in time the job returns to ready.
4. **Why is the attempt number a fencing token?** It rises on every delivery, so a stale worker's ack is detectable (409).
5. **Why accept a late ack?** If nobody else has the job yet, rejecting only causes duplicate work.
6. **Why must the lease record be durable before delivery?** A crash could otherwise reissue the same attempt number and break fencing. We had this bug.
7. **What makes a handler safe to retry?** Idempotency key on its effect, e.g. the job id plus attempt-independent key; demo worker uses an effects dir.
8. **How do you handle a poison job?** Lease exhaustion and failures count as attempts; DLQ after max.
9. **Retry policy?** Exponential backoff, cap, jitter (equal or full). Why jitter: avoids synchronized retries.
10. **Retryable vs non-retryable failure?** The worker marks it; non-retryable goes straight to the DLQ.

## B. Ordering, priority, scheduling

11. **Ordering guarantees?** Per partition, FIFO within a priority class, except retries re-enter. No global order.
12. **Priority without starvation?** Weighted classes 4:2:1, work-conserving.
13. **How are delayed jobs stored?** A min-heap of timers; every fire is a log record.
14. **Scheduler on two instances?** Slot-derived idempotency keys make duplicates collapse.
15. **Hot key?** Same key, same partition; capped by one partition's throughput; demo 10.

## C. Storage

16. **Why a WAL and how is it safe?** CRC32C framing, segments, torn-tail truncation, fsync.
17. **Group commit?** Appenders share an fsync; each waits on a WaitFunc.
18. **What if fsync fails?** Sticky failure; we refuse writes (page cache state is unknowable).
19. **Compaction?** Snapshot plus atomic rename on a single node; disabled when replicated.
20. **Why is replicated compaction unsound as built?** The leader may snapshot uncommitted state that is later truncated.
21. **How do you test recovery?** Property test: replayed state equals live state over random ops including compaction; WAL.Abort for crash.
22. **Why event-sourcing?** One apply function for live and replay.

## D. Replication and elections

23. **Is this Raft?** No. Raft log replication plus an external coordinator for elections. Not consensus.
24. **What is an epoch?** A monotonically increasing leader generation; replicas reject lower epochs.
25. **Why persist the epoch before Prepare?** So a restarted coordinator can never reuse one.
26. **How is the old leader fenced?** Majority durable promises to the new epoch.
27. **Which replica is promoted?** The most up-to-date log among a majority.
28. **Why only count current-epoch entries for commit?** Raft's figure-8 safety rule; leader appends a no-op.
29. **What are leases for?** To bound how long a deposed or isolated leader keeps serving.
30. **Lease clock assumptions?** Durations counted from send time; assumes bounded clock rate drift, not synchronized clocks.
31. **What if the leader's clock freezes?** Stale reads until restart or lease expiry by other means; cannot commit. Tested.
32. **Ack levels?** quorum, leader, all; leader loses writes on leader death (tested).
33. **What happens when a majority is down?** Writes and elections stop. Safe, unavailable.
34. **Why is the coordinator a SPOF and what breaks?** Failovers stop; leaders stop after lease expiry. Two coordinators breaks safety.
35. **How would you fix it?** Consensus-backed control plane.
36. **Split brain test?** Net-partition and freeze demos plus unit tests with the simulated network.

## E. Operations and edge

37. **Backpressure?** Bounded depth, bodies, concurrency; 429/503 with Retry-After.
38. **Why long poll?** Workers control their pace; no registry.
39. **Rate limiting algorithm?** Token buckets at global, tenant, and queue levels; per-process.
40. **Multi-tenancy?** Keys carry tenant and role; idempotency scoped per tenant; producers see only their jobs.
41. **Graceful shutdown?** Stop accepting, drain in-flight within grace, ack, exit 0; gateway closes long polls.
42. **Consumer groups?** Central eager sticky assignment, generation fencing.
43. **What does a rebalance cost?** Whole-group pause; cooperative rebalance is future work.
44. **Observability?** Metrics, structured logs, pprof; see observability.md.
45. **How do you load test honestly?** Open-loop generator to avoid coordinated omission; record environment; one run is not a distribution.

## F. Further More

46. **Biggest correctness risk in this design?** The coordinator's epoch-uniqueness assumption and the lack of a formal model or Jepsen-style testing.
47. **What did testing catch?** Lease-token reuse after crash, acking duplicates before durability, counters lost on recovery, stale heartbeat revoking leadership.
48. **How would you decide whether to build or buy?** Almost always buy (SQS, Kafka, Redis Streams, Temporal) unless you need specific semantics or cost control; this system is a study.
49. **What would you do first to make it production-grade?** Replace the coordinator, add replicated snapshots, security review, upgrade/versioning story, soak and chaos testing on real hardware.
50. **What is wrong with the benchmarks?** Single co-located run on a shared 2-CPU VM, loopback network, no fsync-bound or saturation test.
51. **How would you migrate partition count?** Needs split/merge or dual-write migration; not designed.
52. **How do you prevent a retry storm from the clients?** Respect Retry-After, jittered backoff, retry unknown outcomes only with idempotency keys.
53. **Where could you lose acknowledged data today?** ack=leader on leader loss; disk lying about fsync; simultaneous loss of a majority's disks.
54. **How do you know your lease reasoning is right?** Only through tests and demos, not a proof; the freeze and partition scenarios are the evidence.
55. **What would you monitor to detect a silent failure?** Backlog age growth with flat throughput, leaderless partitions, replica lag.
