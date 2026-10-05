package cluster

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/job"
)

var bg = context.Background()

type tcNode struct {
	n   *Node
	clk *clock.Fake
	dir string
}

// testCluster is a deterministic cluster: control-plane ticks are driven by
// the test, time is fake (per node, so clock faults can be injected), and the
// network is in-memory with fault injection. Replication heartbeats and RPC
// timeouts still use real time.
type testCluster struct {
	t        *testing.T
	net      *Network
	coord    *Coordinator
	coordClk *clock.Fake
	coordDir string
	nodes    map[string]*tcNode
	names    []string
	frozen   map[string]bool
	dead     map[string]bool
	client   *Client
}

func newCluster(t *testing.T, names ...string) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, net: NewNetwork(1), coordClk: clock.NewFake(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)),
		coordDir: t.TempDir(), nodes: map[string]*tcNode{}, names: names, frozen: map[string]bool{}, dead: map[string]bool{}}
	tc.startCoordinator()
	for _, n := range names {
		tc.nodes[n] = &tcNode{clk: clock.NewFake(tc.coordClk.Now()), dir: t.TempDir()}
		tc.startNode(n)
	}
	tc.client = NewClient(ClientConfig{Name: "gw", Transport: tc.net.Endpoint("gw"), Coord: "coord",
		RetryDelay: 5 * time.Millisecond, Retries: 20, RPCTimeout: 2 * time.Second})
	t.Cleanup(func() {
		for n, tn := range tc.nodes {
			if !tc.dead[n] {
				tn.n.Close()
			}
		}
	})
	tc.round(0) // everyone heartbeats once so the coordinator sees them
	return tc
}

func (tc *testCluster) startCoordinator() {
	c, err := NewCoordinator(CoordConfig{Dir: tc.coordDir, Clock: tc.coordClk, Transport: tc.net.Endpoint("coord"),
		FailureTimeout: 3 * time.Second, LeaseDuration: 5 * time.Second, DefaultRF: 3, RPCTimeout: 500 * time.Millisecond})
	if err != nil {
		tc.t.Fatal(err)
	}
	tc.coord = c
	tc.net.Register("coord", c.Handler())
}

func (tc *testCluster) startNode(name string) {
	tn := tc.nodes[name]
	tn.n = NewNode(NodeConfig{Name: name, Dir: tn.dir, Clock: tn.clk, Transport: tc.net.Endpoint(name), Coord: "coord",
		RPCTimeout: 500 * time.Millisecond, ReplHeartbeat: 20 * time.Millisecond})
	tc.net.Register(name, tn.n.Handler())
	delete(tc.dead, name)
}

// round runs one control-plane step: every live node heartbeats, the
// coordinator ticks, then time advances by step on every unfrozen clock.
func (tc *testCluster) round(step time.Duration) {
	for _, name := range tc.names {
		if !tc.dead[name] {
			tc.nodes[name].n.Tick(bg)
		}
	}
	tc.coord.Tick(bg)
	if step > 0 {
		tc.advance(step)
	}
}

// advance moves time forward on the coordinator and on every node that is not frozen.
func (tc *testCluster) advance(d time.Duration) {
	tc.coordClk.Advance(d)
	for n, tn := range tc.nodes {
		if !tc.frozen[n] {
			tn.clk.Advance(d)
		}
	}
}

func (tc *testCluster) until(what string, step time.Duration, maxRounds int, cond func() bool) {
	tc.t.Helper()
	for i := 0; i < maxRounds; i++ {
		if cond() {
			return
		}
		tc.round(step)
		time.Sleep(2 * time.Millisecond) // let replication goroutines run
	}
	if !cond() {
		tc.t.Fatalf("condition never held: %s", what)
	}
}

// settle runs rounds until every partition of every queue has a leader.
func (tc *testCluster) settle() {
	tc.t.Helper()
	tc.until("every partition has a leader", 100*time.Millisecond, 200, func() bool {
		for _, q := range tc.coord.Metadata().Queues {
			for _, p := range q.Partitions {
				if p.Leader == "" {
					return false
				}
			}
		}
		return true
	})
}

func (tc *testCluster) leaderOf(queue string, p int) string {
	return tc.coord.Metadata().Queues[queue].Partitions[p].Leader
}

func (tc *testCluster) createQueue(name string, cfg broker.QueueConfig) {
	tc.t.Helper()
	if _, err := tc.client.CreateQueue(bg, name, cfg); err != nil {
		tc.t.Fatal(err)
	}
	tc.settle()
	tc.round(0)
}

// kill crashes a node (no flush) and removes it from the network.
func (tc *testCluster) kill(name string) {
	tc.nodes[name].n.Kill()
	tc.net.Unregister(name)
	tc.dead[name] = true
}

func (tc *testCluster) restart(name string) {
	tc.startNode(name)
	tc.round(0)
}

func (tc *testCluster) enqueue(queue string, n int, prefix string) []string {
	tc.t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		r, err := tc.client.Enqueue(bg, queue, job.Job{Type: "t", PartitionKey: fmt.Sprint(prefix, i), IdempotencyKey: fmt.Sprint(prefix, i)})
		if err != nil {
			tc.t.Fatalf("enqueue %d: %v", i, err)
		}
		ids = append(ids, r.ID)
	}
	return ids
}

// drain pulls and acks until `want` distinct jobs were seen or rounds run out.
func (tc *testCluster) drain(consumers []*Consumer, want int) map[string]int {
	tc.t.Helper()
	seen := map[string]int{}
	for i := 0; i < 200 && len(seen) < want; i++ {
		for _, c := range consumers {
			jobs, err := c.Poll(bg, 10, time.Minute, 0)
			if err != nil {
				continue
			}
			for _, j := range jobs {
				seen[j.ID]++
				if err := tc.client.Ack(bg, j.ID, j.Attempt); err != nil {
					tc.t.Fatalf("ack %s: %v", j.ID, err)
				}
			}
		}
	}
	return seen
}

func (tc *testCluster) join(queue, group string, members ...string) []*Consumer {
	tc.t.Helper()
	var out []*Consumer
	for _, m := range members {
		c := tc.client.NewConsumer(queue, group, m, 10*time.Second)
		if err := c.Join(bg); err != nil {
			tc.t.Fatal(err)
		}
		out = append(out, c)
	}
	for _, c := range out {
		if _, err := c.Heartbeat(bg); err != nil {
			tc.t.Fatal(err)
		}
	}
	tc.round(0) // nodes learn the new generation
	return out
}

func rf3(parts int) broker.QueueConfig {
	return broker.QueueConfig{Partitions: parts, ReplicationFactor: 3,
		Retry: broker.RetryConfig{MaxAttempts: 3, BaseDelay: broker.Duration(time.Second), MaxDelay: broker.Duration(2 * time.Second), Jitter: "none"}}
}
