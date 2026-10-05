package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

// direct calls a node's handler in-process, bypassing the network. It models
// a client that is on the same side of a partition as that node.
func (tc *testCluster) direct(node, method string, req, resp any, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(bg, timeout)
	defer cancel()
	body, _ := json.Marshal(req)
	out, err := tc.nodes[node].n.Handler()(ctx, "client", method, body)
	if err != nil {
		return err
	}
	if resp != nil {
		raw, _ := json.Marshal(out)
		return json.Unmarshal(raw, resp)
	}
	return nil
}

func statusOf(n *Node, key string) (ReplicaStatus, bool) {
	for _, s := range n.Status() {
		if s.Key == key {
			return s, true
		}
	}
	return ReplicaStatus{}, false
}

// A leader cut off from everyone must not be able to acknowledge a write, even
// while it still believes it is the leader. After the partition heals, its
// uncommitted write must vanish rather than resurface.
func TestIsolatedLeaderCannotAcknowledgeWritesAndItsEntriesAreDiscarded(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(1))
	old := tc.leaderOf("q", 0)
	tc.enqueue("q", 10, "pre")

	tc.net.SetDown(old, true)

	// Still inside its lease the old leader accepts the request but can never
	// reach a quorum, so the caller gets a timeout, never an acknowledgement.
	doomed := job.Job{ID: "q.0." + job.NewID(), Queue: "q", Type: "t", PartitionKey: "doomed", IdempotencyKey: "doomed"}
	err := tc.direct(old, "Node.Enqueue", &EnqueueReq{Key: "q/0", Job: doomed}, nil, 300*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("write at an isolated leader returned %v; it must time out unacknowledged", err)
	}

	// Time passes: the old leader cannot renew, the coordinator detects the
	// failure, waits out the lease, and the majority side elects a new leader.
	tc.until("majority side elected a new leader", time.Second, 60, func() bool {
		l := tc.leaderOf("q", 0)
		return l != "" && l != old
	})
	if st, ok := statusOf(tc.nodes[old].n, "q/0"); !ok || st.Info.Role != replication.Follower {
		t.Fatalf("isolated node still thinks it leads after its lease expired: %+v", st.Info)
	}
	tc.enqueue("q", 5, "post")

	tc.net.SetDown(old, false) // heal
	tc.until("old leader converged", 100*time.Millisecond, 200, func() bool {
		a, _ := statusOf(tc.nodes[old].n, "q/0")
		b, _ := statusOf(tc.nodes[tc.leaderOf("q", 0)].n, "q/0")
		return a.Info.LastIndex == b.Info.LastIndex && a.Info.LastEpoch == b.Info.LastEpoch
	})

	ws := tc.join("q", "g", "w1")
	seen := tc.drain(ws, 100)
	if len(seen) != 15 {
		t.Fatalf("%d jobs exist, want exactly 15 (10 before + 5 after; the unacknowledged one must be gone)", len(seen))
	}
	if _, err := tc.client.Get(bg, doomed.ID); !errors.Is(err, partition.ErrNotFound) {
		t.Fatalf("the unacknowledged job survived the partition: %v", err)
	}
}

// The limit of lease-based leadership, demonstrated: a leader whose clock
// stops (VM pause, clock fault) believes its lease is valid long after the
// coordinator has given leadership to someone else. It can serve STALE READS
// until it is told otherwise, but it still cannot commit a write, because the
// epoch fence and quorum commit do not depend on clocks.
func TestFrozenClockLeaderServesStaleReadsButCannotWrite(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(1))
	old := tc.leaderOf("q", 0)
	r, err := tc.client.Enqueue(bg, "q", job.Job{Type: "t", PartitionKey: "k"})
	if err != nil {
		t.Fatal(err)
	}

	tc.frozen[old] = true // its clock stops
	tc.net.SetDown(old, true)
	tc.until("new leader", time.Second, 60, func() bool { l := tc.leaderOf("q", 0); return l != "" && l != old })

	// On the new leader the job is acknowledged and gone.
	ws := tc.join("q", "g2", "w1")
	got, perr := ws[0].Poll(bg, 5, time.Minute, 0)
	if perr != nil || len(got) != 1 {
		t.Fatalf("poll on new leader: %v %v", got, perr)
	}
	if err := tc.client.Ack(bg, got[0].ID, got[0].Attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := tc.client.Get(bg, r.ID); !errors.Is(err, partition.ErrNotFound) {
		t.Fatalf("job should be gone on the real leader: %v", err)
	}

	// The frozen old leader still serves the pre-failure state: a stale read.
	var jr JobResp
	if err := tc.direct(old, "Node.Get", &JobReq{Key: "q/0", ID: r.ID}, &jr, time.Second); err != nil {
		t.Fatalf("expected the frozen leader to answer (stale) but got %v", err)
	}
	t.Logf("stale read confirmed: deposed leader still reports job %s as %s", jr.Job.ID, jr.Job.State)
	// But it cannot commit anything.
	err = tc.direct(old, "Node.Enqueue", &EnqueueReq{Key: "q/0",
		Job: job.Job{ID: "q.0." + job.NewID(), Queue: "q", Type: "t", PartitionKey: "z"}}, nil, 300*time.Millisecond)
	if err == nil {
		t.Fatal("a deposed leader with a frozen clock acknowledged a write: split brain")
	}

	// Once its clock runs again (or the network heals) it steps down.
	tc.frozen[old] = false
	tc.net.SetDown(old, false)
	tc.until("deposed leader steps down", time.Second, 40, func() bool {
		s, _ := statusOf(tc.nodes[old].n, "q/0")
		return s.Info.Role == replication.Follower
	})
}

func TestCoordinatorOutageStopsLeadersAfterTheirLeaseAndRecoveryNeverReusesEpochs(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(2))
	tc.enqueue("q", 10, "pre")
	ws := tc.join("q", "g", "w1")
	_, _ = ws[0].Assignment()
	epochs := map[int]uint64{}
	for _, p := range tc.coord.Metadata().Queues["q"].Partitions {
		epochs[p.Index] = p.Epoch
	}
	genBefore, _ := ws[0].Assignment()

	// The coordinator dies. Within the lease the data plane keeps working: the
	// control plane is not on the write path.
	tc.net.Unregister("coord")
	tc.enqueue("q", 3, "during-outage")

	// After the lease runs out, leaders must stop: they cannot renew, and a
	// new leader could be elected the moment the coordinator returns.
	for i := 0; i < 8; i++ {
		tc.advance(time.Second)
		for _, n := range tc.names {
			tc.nodes[n].n.Tick(bg)
		}
	}
	tc.client.cfg.Retries = 2
	if _, err := tc.client.Enqueue(bg, "q", job.Job{Type: "t", PartitionKey: "blocked"}); err == nil {
		t.Fatal("write succeeded with no coordinator and expired leases: leaders must stop (this is the cost of the single coordinator)")
	}

	// The coordinator comes back from its persisted state.
	tc.startCoordinator()
	tc.client.cfg.Retries = 30
	// The persisted state still names the old leaders, but they stepped down
	// when their leases ran out; wait for genuinely new elections.
	tc.until("service restored under new epochs", time.Second, 80, func() bool {
		for _, p := range tc.coord.Metadata().Queues["q"].Partitions {
			if p.Leader == "" || p.Epoch <= epochs[p.Index] {
				return false
			}
		}
		return true
	})
	for _, p := range tc.coord.Metadata().Queues["q"].Partitions {
		if p.Epoch <= epochs[p.Index] {
			t.Errorf("partition %d epoch %d did not advance past %d: epochs must never be reused", p.Index, p.Epoch, epochs[p.Index])
		}
	}
	tc.enqueue("q", 3, "after")

	// Group membership was in coordinator memory: the member must rejoin, and
	// its new generation must exceed anything issued before the crash.
	if _, err := ws[0].Heartbeat(bg); err != nil {
		t.Fatal(err)
	}
	if gen, _ := ws[0].Assignment(); gen <= genBefore {
		t.Fatalf("generation %d after coordinator restart; must exceed %d", gen, genBefore)
	}
	seen := tc.drain(ws, 100)
	if len(seen) != 16 {
		t.Fatalf("%d jobs survived the outage, want 16", len(seen))
	}
}

func TestLosingAMajorityStopsElectionsAndWritesThenRecovers(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(1))
	tc.enqueue("q", 8, "pre")
	leader := tc.leaderOf("q", 0)
	var others []string
	for _, n := range tc.names {
		if n != leader {
			others = append(others, n)
		}
	}
	// The leader and one follower die: a single replica remains. It holds all
	// 8 committed jobs, but alone it is not a majority.
	tc.kill(leader)
	tc.kill(others[0])

	for i := 0; i < 20; i++ {
		tc.round(time.Second)
	}
	if l := tc.leaderOf("q", 0); l != "" {
		t.Fatalf("%s was elected leader with only 1 of 3 replicas reachable; an election needs a majority", l)
	}
	if st := tc.coord.Stats(); st.FailedElections == 0 {
		t.Fatalf("no failed elections recorded: %+v", st)
	}
	tc.client.cfg.Retries = 2
	tc.client.cfg.RPCTimeout = 300 * time.Millisecond
	if _, err := tc.client.Enqueue(bg, "q", job.Job{Type: "t", PartitionKey: "x"}); err == nil {
		t.Fatal("write acknowledged with no leader and no quorum")
	}

	tc.restart(others[0]) // a majority (2 of 3) is reachable again
	tc.client.cfg.Retries = 30
	tc.client.cfg.RPCTimeout = 2 * time.Second
	tc.until("leader elected and writes available", time.Second, 80, func() bool {
		_, err := tc.client.Enqueue(bg, "q", job.Job{Type: "t", PartitionKey: "back", IdempotencyKey: "back"})
		return err == nil
	})
	ws := tc.join("q", "g", "w1")
	seen := tc.drain(ws, 100)
	if len(seen) != 9 {
		t.Fatalf("%d jobs after recovery, want 9 (8 acknowledged + 1 new)", len(seen))
	}
}

// The coordinator must promote the replica with the most complete log. Here
// the placement-preferred follower (n2) misses the last writes; n3 has them.
// Promoting n2 would silently drop acknowledged jobs.
func TestElectionPromotesTheMostUpToDateReplica(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(1))
	if got := tc.coord.Metadata().Queues["q"].Partitions[0].Replicas; fmt.Sprint(got) != "[n1 n2 n3]" || tc.leaderOf("q", 0) != "n1" {
		t.Fatalf("unexpected placement %v leader %s", got, tc.leaderOf("q", 0))
	}
	tc.enqueue("q", 10, "early")
	tc.net.Split([]string{"n1"}, []string{"n2"}, true) // n2 stops receiving, n3 and n1 keep going
	ids := tc.enqueue("q", 25, "late")                 // acknowledged by the n1+n3 quorum
	st, _ := statusOf(tc.nodes["n1"].n, "q/0")
	if st.Info.Lag["n2"] < 25 {
		t.Fatalf("n2 should lag by >= 25 entries, lag=%d", st.Info.Lag["n2"])
	}

	tc.kill("n1")
	tc.net.Split([]string{"n1"}, []string{"n2"}, false)
	tc.until("failover", time.Second, 60, func() bool { l := tc.leaderOf("q", 0); return l != "" && l != "n1" })
	if l := tc.leaderOf("q", 0); l != "n3" {
		t.Fatalf("leader = %s; the lagging n2 was promoted over the up-to-date n3", l)
	}
	ws := tc.join("q", "g", "w1")
	seen := tc.drain(ws, 100)
	for _, id := range ids {
		if seen[id] == 0 {
			t.Fatalf("acknowledged job %s lost: a stale replica became leader", id)
		}
	}
}

// The durability dial. With ack=leader a write is acknowledged as soon as the
// leader has it; if the leader dies before replicating, the acknowledged
// write is lost. With ack=quorum the same situation produces a timeout
// instead, never a false acknowledgement.
func TestAckLevelTradesDurabilityForLatency(t *testing.T) {
	run := func(ack string) (acked int, survived int) {
		tc := newCluster(t, "n1", "n2", "n3")
		cfg := rf3(1)
		cfg.Ack = ack
		tc.createQueue("q", cfg)
		leader := tc.leaderOf("q", 0)
		var followers []string
		for _, n := range tc.names {
			if n != leader {
				followers = append(followers, n)
			}
		}
		tc.net.Split([]string{leader}, followers, true) // leader cannot replicate
		tc.client.cfg.Retries = 1
		tc.client.cfg.RPCTimeout = 300 * time.Millisecond
		var ids []string
		for i := 0; i < 5; i++ {
			r, err := tc.client.Enqueue(bg, "q", job.Job{Type: "t", PartitionKey: fmt.Sprint(i)})
			if err == nil {
				acked++
				ids = append(ids, r.ID)
			}
		}
		tc.kill(leader) // the leader dies before it ever replicated
		tc.net.Split([]string{leader}, followers, false)
		tc.until("failover", time.Second, 60, func() bool { l := tc.leaderOf("q", 0); return l != "" && l != leader })
		tc.client.cfg.Retries = 20
		tc.client.cfg.RPCTimeout = 2 * time.Second
		for _, id := range ids {
			if _, err := tc.client.Get(bg, id); err == nil {
				survived++
			}
		}
		return
	}
	a, s := run("leader")
	if a != 5 || s != 0 {
		t.Errorf("ack=leader: acknowledged %d, survived %d; expected 5 acknowledged and 0 survivors (data loss)", a, s)
	}
	t.Logf("ack=leader: %d writes acknowledged, %d survived the leader's death", a, s)
	a, s = run("quorum")
	if a != 0 || s != 0 {
		t.Errorf("ack=quorum: acknowledged %d, survived %d; expected writes to time out rather than be falsely acknowledged", a, s)
	}
	t.Logf("ack=quorum: %d writes acknowledged (they timed out instead), so nothing acknowledged was lost", a)
}

func TestReplicaLagIsVisibleOnTheLeader(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(1))
	leader := tc.leaderOf("q", 0)
	var slow string
	for _, n := range tc.names {
		if n != leader {
			slow = n
		}
	}
	tc.net.SetDown(slow, true)
	tc.enqueue("q", 40, "k")
	st, _ := statusOf(tc.nodes[leader].n, "q/0")
	if st.Info.Lag[slow] < 40 {
		t.Fatalf("lag for %s = %d, want >= 40", slow, st.Info.Lag[slow])
	}
	tc.net.SetDown(slow, false)
	tc.until("lag drains", 50*time.Millisecond, 200, func() bool {
		s, _ := statusOf(tc.nodes[leader].n, "q/0")
		return s.Info.Lag[slow] == 0
	})
}

func TestQueueDeletionRemovesReplicasEverywhere(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(2))
	tc.enqueue("q", 5, "k")
	if err := tc.client.DeleteQueue(bg, "q"); err != nil {
		t.Fatal(err)
	}
	tc.until("replicas dropped", 100*time.Millisecond, 50, func() bool {
		for _, tn := range tc.nodes {
			if len(tn.n.Status()) != 0 {
				return false
			}
		}
		return true
	})
	if _, err := tc.client.Enqueue(bg, "q", job.Job{Type: "t"}); !errors.Is(err, broker.ErrQueueNotFound) {
		t.Fatalf("enqueue to deleted queue: %v", err)
	}
	// Re-creating the queue must not resurrect old jobs.
	tc.createQueue("q", rf3(2))
	ws := tc.join("q", "g", "w1")
	if seen := tc.drain(ws[:1], 1); len(seen) != 0 {
		t.Fatalf("recreated queue contains %d old jobs", len(seen))
	}
}
