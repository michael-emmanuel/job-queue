# Design notes: Milestone 3 (distribution)

Replication, leader election, failure detection and consumer groups. Every
claim below names the test that demonstrates it; claims without a test are
labelled as such. Sections 2 and 3 matter most.

## 1. Architecture

```
                     +---------------------------+
                     |        Coordinator        |   control plane
                     |  placement, epochs,       |   (single process)
                     |  leases, failure detector,|
                     |  consumer groups          |
                     +-----^---------^-----------+
          heartbeat/lease    |         |  metadata, group RPCs
        +--------------------+         +--------------------+
        |                                                   |
  +-----v------+      AppendEntries       +------------+  +-v--------+
  |   Node n1  |<------------------------>|   Node n2  |  |  Client  |
  | q/0 LEADER |                          | q/0 follower| | (gateway |
  | q/1 follower|<----------------------->| q/1 LEADER |  |  logic)  |
  +-----^------+                          +------------+  +-+--------+
        |      data plane: enqueue / pull / ack go to the     |
        +-----------------------leader----------------------+
```

- **Data plane**: a client resolves a partition's leader from cached metadata
  and talks to it. The leader appends to its log, replicates to followers,
  and acknowledges per the queue's ack level.
- **Control plane**: the coordinator is NOT on the write path. It places
  partitions, detects failed nodes, runs elections and grants leases.
- **Followers hold only a log** (no in-memory queue), so they are cheap.
  A new leader rebuilds queue state by replaying the log.

All parts talk through one `Transport` interface (`Call(ctx, to, method, req,
resp)`). Tests use an in-memory network that can cut links, drop and duplicate
messages and delay them; every message is JSON-encoded so no component shares
memory with another. **No real network transport exists yet** (Milestone 4),
so behaviour over real sockets is untested; the simulated network delivers
messages synchronously and never reorders them.

## 2. Replication, consensus and leader election are three different things

- **Replication** copies data to several machines. It says nothing about who
  is allowed to write or what happens when copies disagree.
- **Leader election** picks one writer. Done carelessly it produces two (split
  brain).
- **Consensus** is the agreement protocol that makes election and commit
  safe when machines and networks fail. Raft and Paxos are consensus.

This repository implements **Raft's log-replication half** (epochs, log
matching, truncation of divergent suffixes, quorum commit, the "commit only
current-epoch entries by counting" rule, a no-op at election) and replaces
the election half with an **external coordinator** that assigns epochs. That
is closer to Viewstamped Replication with an outside view-change driver than
to Raft. It is NOT a consensus implementation, and it must not be described
as one.

### Why committed writes survive a failover

1. A write is committed when a majority of replicas has fsynced it.
2. To elect, the coordinator persists a new epoch, then asks every replica to
   `Prepare` it. A replica durably promises the epoch (fsync before replying)
   and from then on rejects every message from a lower epoch. If it was
   leading, it steps down.
3. The coordinator needs promises from a **majority**. Any committed entry is
   on a majority, and two majorities intersect, so at least one promising
   replica holds every committed entry.
4. Because those replicas have promised, the old leader can no longer commit
   anything new, so the reported log positions cannot grow behind the
   coordinator's back.
5. The coordinator promotes the promising replica with the highest
   (last epoch, last index). It holds every committed entry. It appends a
   no-op in its epoch so older entries become committed, and reconciles the
   followers, truncating any entry that conflicts with its history.

Tests: `TestAcknowledgedWritesNeverLostUnderRandomFailovers` (random crashes
and elections, 8 seeds), `TestLeaderFailoverLosesNoAcknowledgedJob`,
`TestElectionPromotesTheMostUpToDateReplica`.

Mutation checks (rule deliberately broken, tests must fail; all were caught):
no truncation of conflicting entries; commit as soon as the leader has an
entry; `Prepare` that does not fence the old leader; elections that need no
majority; a coordinator that prefers the shorter log; a coordinator that
ignores the old leader's lease. Two of these first slipped through weak tests
and were tightened; that is why those tests are as specific as they are.

### What the safety argument depends on

The argument is sound only if **at most one coordinator exists and it never
issues the same epoch to two candidates**. The epoch is persisted before use
and the process is single, so that holds here. It would NOT hold if an operator
started two coordinators with separate state: both could issue epoch N+1, two
replicas could lead the same epoch, and their logs could diverge without
either noticing. Production systems remove this by keeping the epoch in a
consensus store (etcd, ZooKeeper) or by running election inside the brokers
with Raft. A cheap hardening not implemented here: a replica remembers which
leader it accepted for an epoch and rejects a different leader for the same
epoch.

## 3. Split brain, stale leaders, leases and fencing

| Mechanism | What it prevents | What it does not |
|---|---|---|
| Quorum commit | An isolated leader acknowledging a write | Serving reads from its memory |
| Epoch fencing (`Prepare`/AppendEntries rejection) | A deposed leader committing or being believed | A deposed leader that has not yet heard |
| Leases | A deposed leader serving reads for long | Anything if clocks run at different RATES |
| Truncation on reconciliation | An uncommitted entry resurfacing | Nothing; it is exact |

**Isolated leader** (`TestIsolatedLeaderCannotAcknowledgeWritesAndItsEntriesAreDiscarded`):
a leader cut off from everyone still accepts a request while its lease holds,
but the caller gets a timeout, never an acknowledgement. The majority side
elects a new leader and keeps working. When the partition heals the old
leader learns the newer epoch, steps down, throws away its in-memory queue
(which contained effects of writes that never committed), and its
uncommitted entry is replaced by the new history. Exactly the 15 acknowledged
jobs exist afterwards; the doomed job does not.

**Frozen clock** (`TestFrozenClockLeaderServesStaleReadsButCannotWrite`): a
leader whose clock stops (VM pause, clock fault) believes its lease is valid
after the coordinator has moved on. It answered a status read with a job that
the real leader had already completed: **a stale read, demonstrated**. It
still could not commit a write, because fencing and quorum commit do not use
clocks. So: leases bound staleness only under bounded clock-RATE drift and no
unbounded pauses; writes do not depend on clocks at all.

**Lease timing.** The coordinator grants a lease from the moment it receives a
heartbeat; the node counts it from the moment it sent that heartbeat. The
node's view therefore always ends first, and no clock needs to agree with
another, only run at about the same speed. Leases travel as durations. The
price is failover latency: an election waits for the old leader's lease, so
failover takes about `max(failure timeout, lease)`. In the tests (3s and 5s)
it took 5.5s of cluster time, asserted to be at least the lease
(`TestLeaderFailoverLosesNoAcknowledgedJob`).

## 4. Failure detection is inherently imperfect

A silent node may be crashed, slow, paused or partitioned, and from outside
those cannot be told apart. The coordinator declares a node dead after
`FailureTimeout` without a heartbeat. Too short: a GC pause or busy node
triggers a needless failover (expensive, and it can flap). Too long: real
failures go unnoticed. Either way **safety never depends on the detector being
right**, because fencing and quorum commit protect the data; a wrong
detection only costs availability. The detector is purely a liveness tool.

## 5. Acknowledgement levels

Per queue (`ack`), tested in `TestAckLevelTradesDurabilityForLatency` with the
leader cut off from its followers and then killed:

| Level | Acknowledged when | Result in the test | Cost |
|---|---|---|---|
| `leader` | leader fsynced | 5 of 5 writes acknowledged, **0 survived** | lowest latency; acknowledged data can be lost |
| `quorum` (default) | majority fsynced | 0 acknowledged (timed out), nothing lost | one replication round trip; unavailable without a majority |
| `all` | every replica fsynced | stalls if any replica is down | worst availability |

**A timeout is not a failure.** A write that timed out because the quorum was
gone was already appended on the leader; when the quorum returned it
committed (`TestLosingAMajorityStopsElectionsAndWritesThenRecovers` accepts
both outcomes and logs which one happened). Clients must retry such writes
with an idempotency key. The client library retries automatically only when
the error proves the request was not applied (not leader, not hosted,
unreachable), or when the operation is idempotent.

Without a majority nothing can be elected or committed
(`TestLosingAMajorityStops...`): the system chooses consistency over
availability for that partition.

## 6. Consumer groups

A group is a set of workers that share a queue's partitions. Job leases
already make each job exclusive, so groups are not needed for correctness;
they provide affinity (one owner per partition), balance, and no thundering
herd of every worker polling every partition.

**Protocol.** `Join` registers a member; heartbeats keep it alive; a member
silent for its session timeout is removed. Any membership change bumps the
group **generation** and recomputes the assignment centrally. Members learn
the new generation and partitions on their next heartbeat.

**Assignment** is balanced (sizes differ by at most one) and sticky (existing
ownership is kept unless balance demands a move, so a join or leave moves the
fewest partitions). The six-partition, three-worker example from the spec
yields 2/2/2, and when a worker dies only its partitions move. Verified by
`TestAssignmentProperties` (500 random cases: full coverage, no duplicates,
balance, determinism) and `TestWorkerDeathMovesOnlyItsPartitions`.

**Eager vs cooperative.** This is the eager scheme: one computation, everyone
switches together after at most one heartbeat. Kafka's incremental cooperative
rebalancing exists because its coordinator cannot compute globally; with a
central computation the stop-the-world pause is just a heartbeat interval.

**Fencing.** Pulls carry the member's generation, and nodes learn current
generations from heartbeats and reject older ones
(`TestStaleGenerationIsFencedByNodes`); the consumer recovers by refreshing.
What is and is not guaranteed: **normally one active consumer per partition**.
For up to one node-heartbeat after a rebalance a stale member may still pull,
so two members can briefly overlap on a partition. They still cannot hold the
same job, because the lease is the real guard.

**Worker death** (`TestWorkerDeathRebalancesAndItsJobsAreRedelivered`): a
worker leases jobs and vanishes. After its session lapses its partitions move
to the survivors, and after the jobs' visibility timeout every one of them is
redelivered and completed. The session timeout controls how long a dead
worker's partitions sit idle; the visibility timeout controls how long its
in-flight jobs do.

**Coordinator restart.** Group membership lives in coordinator memory, so
members get `unknown member` and rejoin. Generations must not repeat, or a
stale member would look current, so the highest generation per group is
persisted (`TestGenerationNeverReusedAfterCoordinatorRestart`,
`TestCoordinatorOutage...`).

## 7. Recovery paths

- **Follower restart**: reopens its log, reports its position, and the leader
  sends what is missing in batches, walking back on mismatch
  (`TestRestartedNodeRejoinsAsFollowerAndCatchesUp`,
  `TestLaggingFollowerCatchesUpAfterLongOutage` with 1,500 entries).
- **Leader change**: the new leader replays its whole log into a fresh
  partition. **Failover time therefore grows with log length**, and because
  compaction is disabled for replicated partitions, that length only grows.
  Unmeasured; this is the first scaling limit to fix.
- **Deposed leader**: discards its in-memory state instead of repairing it,
  because the leader applies writes speculatively before commit.
- **Message loss and duplication**: a 15% loss, 20% duplication network with
  application-level retries produced exactly one job per idempotency key
  (`TestIdempotentEnqueueSurvivesLossAndDuplication`). Duplicated or reordered
  AppendEntries are harmless: matching entries are skipped and never truncate
  (`TestMergeSkipsDuplicatesAndNeverTruncatesMatching`).

## 8. The coordinator is a single point of failure

`TestCoordinatorOutage...` shows the behaviour. While it is down the data plane
keeps working until leases run out (writes succeeded during the outage). After
`LeaseDuration` leaders step down by themselves and writes fail: **availability
is lost, correctness is not.** When the coordinator returns from its persisted
state, elections run under strictly higher epochs, so no epoch is reused.
Failover, lease renewal, queue creation and group rebalancing are all
unavailable during the outage. The fix is a replicated coordinator.

## 9. Observability added

Per partition: leader flag, epoch, last/durable/commit index, lag per follower
(`jobqueue_replica_lag_entries`). Cluster: alive nodes, elections (leader
changes), failed elections, leaderless partitions, partition epochs, group
members, generations and rebalances. `TestClusterMetricsExposeReplicationAndFailover`.

Alerts worth having: `jobqueue_leaderless_partitions > 0` for more than a lease;
a rising `jobqueue_failed_elections_total` (lost quorum); sustained replica lag;
frequent leader changes (flapping, usually a too-aggressive failure timeout).

## 10. Known limitations

- Single coordinator (above) and no authentication between nodes.
- **No log compaction or InstallSnapshot for replicated partitions.** Snapshots
  of speculative state are unsound, so compaction would need to snapshot at the
  commit point. Logs and failover times grow without bound.
- **No replica reassignment.** Placement is fixed at creation; a permanently
  lost node leaves partitions under-replicated until an operator intervenes.
- **No follower reads.** Every read goes to the leader.
- Job status reads on the leader include effects of writes not yet committed.
- Failover rebuilds state by full log replay.
- Tested only on a simulated network with three nodes, and with real-time
  replication goroutines on a single machine; no cross-machine run, no
  throughput or latency measurements.
- Each partition runs one replicator goroutine per follower; goroutine count
  scales with partitions x followers.
- The same-epoch two-leader hazard (section 2) is documented but not guarded.

## 11. What would break first at scale

1. **Failover time**, from log replay with no compaction.
2. **The coordinator**: every node heartbeats it, every election runs through
   it, and it is a single process.
3. **Per-partition replicators and fsyncs**: cost grows linearly with the
   partition count per node.
4. **Leader skew** after failures: leaders do not rebalance back, so one node
   can end up leading far more partitions than others.
