package cluster

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

func TestPlacementAndElection(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(6))
	meta := tc.coord.Metadata().Queues["q"]
	perNode := map[string]int{}
	for _, p := range meta.Partitions {
		if len(p.Replicas) != 3 || p.Leader == "" || p.Epoch == 0 {
			t.Fatalf("partition %d: %+v", p.Index, p)
		}
		perNode[p.Leader]++
	}
	for n, c := range perNode {
		if c != 2 {
			t.Errorf("node %s leads %d of 6 partitions; placement should spread leaders evenly", n, c)
		}
	}
	if _, err := tc.client.CreateQueue(bg, "q", rf3(2)); !errors.Is(err, broker.ErrQueueExists) {
		t.Errorf("duplicate create: %v", err)
	}
	cfg := rf3(2)
	cfg.ReplicationFactor = 5
	if _, err := tc.client.CreateQueue(bg, "too-wide", cfg); !errors.Is(err, broker.ErrInvalidQueue) {
		t.Errorf("RF 5 on 3 nodes: %v", err)
	}
}

func TestEndToEndThroughReplicatedPartitions(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(6))
	ids := tc.enqueue("q", 30, "k")
	ws := tc.join("q", "workers", "w1", "w2", "w3")

	for _, w := range ws {
		_, parts := w.Assignment()
		if len(parts) != 2 {
			t.Fatalf("worker has %d partitions, want 2", len(parts))
		}
	}
	seen := tc.drain(ws, 30)
	if len(seen) != 30 {
		t.Fatalf("processed %d of 30 jobs", len(seen))
	}
	for _, id := range ids {
		if seen[id] != 1 {
			t.Errorf("job %s delivered %d times", id, seen[id])
		}
	}
	// Every follower holds the same log as its leader once replication settles.
	tc.until("replicas converged", 50*time.Millisecond, 100, func() bool {
		for _, tn := range tc.nodes {
			for _, st := range tn.n.Status() {
				if st.Info.Role == replication.Leader {
					for _, lag := range st.Info.Lag {
						if lag != 0 {
							return false
						}
					}
				}
			}
		}
		return true
	})
}

func TestWorkersOnlyReceiveTheirOwnPartitions(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(6))
	tc.enqueue("q", 60, "k")
	ws := tc.join("q", "g", "w1", "w2")
	for _, w := range ws {
		_, mine := w.Assignment()
		own := map[int]bool{}
		for _, p := range mine {
			own[p] = true
		}
		jobs, err := w.Poll(bg, 100, time.Minute, 0)
		if err != nil || len(jobs) == 0 {
			t.Fatalf("poll: %d %v", len(jobs), err)
		}
		for _, j := range jobs {
			_, p, _ := broker.ParseID(j.ID)
			if !own[p] {
				t.Fatalf("worker received a job from partition %d it does not own (%v)", p, mine)
			}
		}
	}
}

func TestLeaderFailoverLosesNoAcknowledgedJob(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(3))
	ids := tc.enqueue("q", 60, "k") // every one acknowledged at quorum

	victim := tc.leaderOf("q", 0)
	tc.kill(victim)

	// Failure detection (3s) plus the old lease (5s) must pass before a new
	// leader can be elected: that is the price of the lease.
	start := tc.coordClk.Now()
	tc.until("new leaders elected", 500*time.Millisecond, 100, func() bool {
		for _, p := range tc.coord.Metadata().Queues["q"].Partitions {
			if p.Leader == "" || p.Leader == victim {
				return false
			}
		}
		return true
	})
	failover := tc.coordClk.Now().Sub(start)
	// The election must wait out the dead leader's lease (5s), not just detect
	// the failure (3s): until then the old leader may still be serving reads.
	// The slack covers the last heartbeat landing a little before the kill.
	if failover < 4500*time.Millisecond || failover > 10*time.Second {
		t.Errorf("failover took %v of cluster time; expected about the 5s lease, not just the 3s failure detector", failover)
	}
	t.Logf("failover completed after %v of cluster time", failover)

	ws := tc.join("q", "g", "w1", "w2", "w3")
	seen := tc.drain(ws, 60)
	for _, id := range ids {
		if seen[id] == 0 {
			t.Fatalf("job %s was acknowledged before the failure but lost by it", id)
		}
	}
	// Writes work again on the new leaders.
	if _, err := tc.client.Enqueue(bg, "q", job.Job{Type: "t", PartitionKey: "after"}); err != nil {
		t.Fatalf("enqueue after failover: %v", err)
	}
}

func TestRestartedNodeRejoinsAsFollowerAndCatchesUp(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(3))
	tc.enqueue("q", 20, "a")
	victim := tc.leaderOf("q", 0)
	tc.kill(victim)
	tc.until("failover", 500*time.Millisecond, 100, func() bool { return tc.leaderOf("q", 0) != "" && tc.leaderOf("q", 0) != victim })
	tc.enqueue("q", 40, "b") // written while the victim is down

	tc.restart(victim)
	tc.until("restarted node caught up", 100*time.Millisecond, 200, func() bool {
		for _, st := range tc.nodes[victim].n.Status() {
			l := tc.nodes[tc.leaderOf("q", partOf(st.Key))].n
			for _, ls := range l.Status() {
				if ls.Key == st.Key && ls.Info.Role == replication.Leader && st.Info.LastIndex != ls.Info.LastIndex {
					return false
				}
			}
		}
		return len(tc.nodes[victim].n.Status()) == 3
	})
}

func partOf(key string) int { _, i := splitKey(key); return i }

func TestWorkerDeathRebalancesAndItsJobsAreRedelivered(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(6))
	ids := tc.enqueue("q", 24, "k")
	ws := tc.join("q", "g", "w1", "w2", "w3")
	genBefore, _ := ws[0].Assignment()

	// w3 pulls its jobs and then dies without acking or heartbeating.
	dead := ws[2]
	took, err := dead.Poll(bg, 100, 3*time.Second, 0) // 3s visibility
	if err != nil || len(took) == 0 {
		t.Fatalf("w3 took %d jobs: %v", len(took), err)
	}

	// Survivors keep heartbeating; w3's session (10s) lapses.
	for i := 0; i < 14; i++ {
		tc.round(time.Second)
		for _, w := range ws[:2] {
			if _, err := w.Heartbeat(bg); err != nil {
				t.Fatal(err)
			}
		}
	}
	tc.round(0)
	for _, w := range ws[:2] {
		gen, parts := w.Assignment()
		if gen <= genBefore || len(parts) != 3 {
			t.Fatalf("after w3's death: generation %d (was %d), %d partitions; want 3 each", gen, genBefore, len(parts))
		}
	}
	if st := tc.coord.Stats(); st.Rebalances < 4 {
		t.Errorf("rebalances = %d", st.Rebalances)
	}

	seen := tc.drain(ws[:2], 24)
	for _, id := range ids {
		if seen[id] == 0 {
			t.Fatalf("job %s stranded with the dead worker", id)
		}
	}
	redelivered := 0
	for _, j := range took {
		if seen[j.ID] > 0 {
			redelivered++
		}
	}
	if redelivered != len(took) {
		t.Fatalf("only %d of the dead worker's %d jobs were redelivered", redelivered, len(took))
	}
}

func TestStaleGenerationIsFencedByNodes(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(2))
	tc.enqueue("q", 10, "k")
	ws := tc.join("q", "g", "w1")
	staleGen, parts := ws[0].Assignment()

	// A second member joins: generation advances and nodes learn about it.
	tc.join("q", "g", "w2")
	var r PullResp
	err := tc.client.onLeader(bg, "q", parts[0], "Node.Pull", &PullReq{Key: Key("q", parts[0]), Group: "g",
		Member: "w1", Generation: staleGen, Worker: "w1", Max: 5}, &r, false)
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("pull with a stale generation = %v, want ErrStaleGeneration", err)
	}
	// The consumer notices on its own and recovers.
	jobs, err := ws[0].Poll(bg, 5, time.Minute, 0)
	if err != nil {
		t.Fatalf("poll after fencing: %v", err)
	}
	_ = jobs // may be empty: the point is that the consumer recovered
	if gen, _ := ws[0].Assignment(); gen <= staleGen {
		t.Fatalf("consumer did not adopt the new generation: %d", gen)
	}
}

func TestIdempotentEnqueueSurvivesLossAndDuplication(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(3))
	tc.net.SetLoss(0.15)
	tc.net.SetDuplicate(0.2)
	tc.client.cfg.Retries = 60
	const n = 40
	ids := map[string]bool{}
	for i := 0; i < n; i++ {
		var r broker.EnqueueResult
		var err error
		for try := 0; try < 5; try++ { // the application retries on error, as it must
			r, err = tc.client.Enqueue(bg, "q", job.Job{Type: "t", IdempotencyKey: fmt.Sprint("order-", i)})
			if err == nil {
				break
			}
		}
		if err != nil {
			t.Fatalf("enqueue %d: %v", i, err)
		}
		ids[r.ID] = true
	}
	tc.net.SetLoss(0)
	tc.net.SetDuplicate(0)
	if len(ids) != n {
		t.Fatalf("%d distinct job IDs for %d logical orders", len(ids), n)
	}
	ws := tc.join("q", "g", "w1")
	seen := tc.drain(ws, n)
	if len(seen) != n {
		t.Fatalf("%d of %d jobs exist", len(seen), n)
	}
	var dup []string
	for id, c := range seen {
		if c != 1 {
			dup = append(dup, id)
		}
	}
	sort.Strings(dup)
	if len(dup) > 0 {
		t.Fatalf("duplicated jobs despite idempotency keys: %v", dup)
	}
}

func TestClusterMetricsExposeReplicationAndFailover(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(2))
	tc.enqueue("q", 10, "k")
	tc.join("q", "g", "w1", "w2")

	reg := observability.NewRegistry()
	reg.AddCollector(tc.coord.Collect)
	for _, tn := range tc.nodes {
		reg.AddCollector(tn.n.Collect)
	}
	render := func() string {
		var b strings.Builder
		if err := reg.Expose(&b); err != nil {
			t.Fatal(err)
		}
		return b.String()
	}
	out := render()
	for _, want := range []string{
		`jobqueue_replica_is_leader{node="n1",partition="q/0"} 1`,
		`jobqueue_replica_is_leader{node="n2",partition="q/0"} 0`,
		`jobqueue_replica_lag_entries{node="n1",partition="q/0",follower="n2"}`,
		`jobqueue_replica_commit_index{node=`,
		`jobqueue_cluster_alive_nodes 3`,
		`jobqueue_leader_elections_total 2`,
		`jobqueue_leaderless_partitions 0`,
		`jobqueue_group_members{queue="q",group="g"} 2`,
		`jobqueue_partition_epoch{queue="q",partition="0"} 1`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("metrics missing %q", want)
		}
	}
	victim := tc.leaderOf("q", 0)
	tc.kill(victim)
	tc.until("failover", 500*time.Millisecond, 100, func() bool { l := tc.leaderOf("q", 0); return l != "" && l != victim })
	out = render()
	if !strings.Contains(out, `jobqueue_leader_elections_total 3`) || !strings.Contains(out, `jobqueue_partition_epoch{queue="q",partition="0"} 2`) {
		t.Errorf("failover not reflected in metrics:\n%s", out)
	}
}

// A heartbeat can be built just before a Promote lands and delivered just
// after the coordinator has recorded the new leader. It then legitimately
// lacks the partition, and must NOT be read as "the leader stepped down":
// doing so revoked leadership that had just been granted, causing a spurious
// second election (and a lease-length outage) right after every startup.
func TestStaleHeartbeatDoesNotRevokeFreshLeadership(t *testing.T) {
	tc := newCluster(t, "n1", "n2", "n3")
	tc.createQueue("q", rf3(1))
	leader := tc.leaderOf("q", 0)
	epoch := tc.coord.Metadata().Queues["q"].Partitions[0].Epoch
	if epoch != 1 {
		t.Fatalf("first election should be epoch 1, got %d (a spurious re-election already happened)", epoch)
	}
	// The racing heartbeat: built before the promotion, so it does not list the partition.
	tc.coord.heartbeat(HeartbeatReq{Node: leader})
	if got := tc.leaderOf("q", 0); got != leader {
		t.Fatalf("leader became %q after a stale heartbeat; fresh leadership must be given time to show up", got)
	}

	// But a heartbeat sent well AFTER the promotion that still omits the
	// partition means the node really lost it (it restarted or stepped
	// down), and the coordinator must notice, or the partition would be
	// leaderless forever.
	tc.advance(5 * time.Second)
	tc.coord.heartbeat(HeartbeatReq{Node: leader})
	if got := tc.leaderOf("q", 0); got != "" {
		t.Fatalf("a restarted leader's omission was ignored; leader is still %q", got)
	}
}
