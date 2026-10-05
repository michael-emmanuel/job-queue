package replication

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

// tnet is a tiny in-memory network: every message is JSON round-tripped (no
// shared memory) and links can be cut.
type tnet struct {
	mu   sync.Mutex
	reps map[string]*Replica
	down map[string]bool    // node fully isolated
	cut  map[[2]string]bool // directed link cut
}

func (n *tnet) transport(from string) Transport { return tp{n, from} }

type tp struct {
	n    *tnet
	from string
}

func (t tp) Call(ctx context.Context, to, method string, req, resp any) error {
	n := t.n
	n.mu.Lock()
	r := n.reps[to]
	blocked := n.down[t.from] || n.down[to] || n.cut[[2]string{t.from, to}] || r == nil
	n.mu.Unlock()
	if blocked {
		return errors.New("unreachable")
	}
	raw, _ := json.Marshal(req)
	var out any
	var err error
	switch method {
	case "Replica.AppendEntries":
		var rq AppendReq
		json.Unmarshal(raw, &rq)
		out, err = r.HandleAppend(ctx, &rq)
	case "Replica.Prepare":
		var rq PrepareReq
		json.Unmarshal(raw, &rq)
		out, err = r.HandlePrepare(&rq)
	default:
		return fmt.Errorf("unknown method %s", method)
	}
	if err != nil {
		return err
	}
	b, _ := json.Marshal(out)
	return json.Unmarshal(b, resp)
}

func (n *tnet) isolate(id string, v bool) { n.mu.Lock(); n.down[id] = v; n.mu.Unlock() }

type group struct {
	t     *testing.T
	net   *tnet
	ids   []string
	dirs  map[string]string
	clk   *clock.Fake
	ack   AckLevel
	steps map[string]*atomic.Int64 // step-down callbacks seen per node
}

func newGroup(t *testing.T, ack AckLevel, ids ...string) *group {
	t.Helper()
	g := &group{t: t, net: &tnet{reps: map[string]*Replica{}, down: map[string]bool{}, cut: map[[2]string]bool{}},
		ids: ids, dirs: map[string]string{}, clk: clock.NewFake(time.Now()), ack: ack, steps: map[string]*atomic.Int64{}}
	for _, id := range ids {
		g.dirs[id] = t.TempDir()
		g.steps[id] = &atomic.Int64{}
		g.start(id)
	}
	t.Cleanup(func() {
		g.net.mu.Lock()
		reps := make([]*Replica, 0)
		for _, r := range g.net.reps {
			reps = append(reps, r)
		}
		g.net.mu.Unlock()
		for _, r := range reps {
			r.Close()
		}
	})
	return g
}

func (g *group) start(id string) *Replica {
	g.t.Helper()
	r, err := OpenReplica(Config{ID: id, Key: "q/0", Path: filepath.Join(g.dirs[id], "rlog"), Clock: g.clk,
		Transport: g.net.transport(id), Ack: g.ack, Heartbeat: 20 * time.Millisecond, RPCTimeout: 500 * time.Millisecond,
		OnStepDown: func(uint64) { g.steps[id].Add(1) }})
	if err != nil {
		g.t.Fatal(err)
	}
	g.net.mu.Lock()
	g.net.reps[id] = r
	g.net.mu.Unlock()
	return r
}

func (g *group) rep(id string) *Replica {
	g.net.mu.Lock()
	defer g.net.mu.Unlock()
	return g.net.reps[id]
}

// crash kills a replica without flushing and removes it from the network.
func (g *group) crash(id string) {
	r := g.rep(id)
	r.log.Abort()
	r.mu.Lock()
	r.stepDownLocked(r.epoch)
	r.mu.Unlock()
	g.net.mu.Lock()
	delete(g.net.reps, id)
	g.net.mu.Unlock()
}

// elect plays the coordinator: promise epoch on every reachable replica,
// require a majority, pick the most up-to-date log, and promote it.
func (g *group) elect(epoch uint64) string {
	g.t.Helper()
	type pos struct {
		id           string
		lastE, lastI uint64
	}
	var oks []pos
	for _, id := range g.ids {
		var resp PrepareResp
		if err := g.net.transport("coord").Call(context.Background(), id, "Replica.Prepare", &PrepareReq{Key: "q/0", Epoch: epoch}, &resp); err == nil && resp.OK {
			oks = append(oks, pos{id, resp.LastEpoch, resp.LastIndex})
		}
	}
	if len(oks) < len(g.ids)/2+1 {
		g.t.Fatalf("no majority promised epoch %d (%d of %d)", epoch, len(oks), len(g.ids))
	}
	best := oks[0]
	for _, p := range oks[1:] {
		if p.lastE > best.lastE || p.lastE == best.lastE && p.lastI > best.lastI {
			best = p
		}
	}
	if err := g.rep(best.id).StartLeading(epoch, g.ids, g.clk.Now().Add(time.Hour)); err != nil {
		g.t.Fatal(err)
	}
	return best.id
}

func write(r *Replica, s string, timeout time.Duration) error {
	lg, err := r.LeaderLog()
	if err != nil {
		return err
	}
	wait, err := lg.Append(storage.Record{Type: 1, Data: []byte(s)})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return wait(ctx)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func contents(t *testing.T, r *Replica) []string {
	t.Helper()
	var out []string
	r.log.Replay(func(e Entry) error {
		if e.Type != TypeNoop {
			out = append(out, string(e.Data))
		}
		return nil
	})
	return out
}

func TestQuorumWriteReplicatesToAllFollowers(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	leader := g.rep(g.elect(1))
	for i := 0; i < 100; i++ {
		if err := write(leader, fmt.Sprint("w", i), 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	info := leader.Info()
	if info.Role != Leader || info.Commit < 100 {
		t.Fatalf("leader info: %+v", info)
	}
	for _, id := range g.ids {
		id := id
		eventually(t, id+" caught up", func() bool { return g.rep(id).log.LastIndex() == info.LastIndex })
	}
	want := contents(t, leader)
	for _, id := range g.ids {
		if got := contents(t, g.rep(id)); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s diverged", id)
		}
	}
}

func TestCommitSurvivesOneDeadFollowerButNotTwo(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	leader := g.rep(g.elect(1))
	if err := write(leader, "x", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	other := "b"
	if leader.cfg.ID == "b" {
		other = "a"
	}
	g.net.isolate(other, true)
	if err := write(leader, "one follower down", 5*time.Second); err != nil {
		t.Fatalf("a quorum of 2/3 must still commit: %v", err)
	}
	for _, id := range g.ids {
		if id != leader.cfg.ID {
			g.net.isolate(id, true)
		}
	}
	err := write(leader, "no quorum", 300*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("write without quorum = %v, want a timeout (unavailable, not wrongly acknowledged)", err)
	}
}

func TestAckLevels(t *testing.T) {
	// AckLeader acknowledges with no followers reachable; AckAll refuses.
	for _, c := range []struct {
		ack  AckLevel
		want bool
	}{{AckLeader, true}, {AckQuorum, false}, {AckAll, false}} {
		g := newGroup(t, c.ack, "a", "b", "c")
		leader := g.rep(g.elect(1))
		for _, id := range g.ids {
			if id != leader.cfg.ID {
				g.net.isolate(id, true)
			}
		}
		err := write(leader, "x", 200*time.Millisecond)
		if (err == nil) != c.want {
			t.Errorf("ack level %d with all followers down: err=%v, acknowledged=%v want %v", c.ack, err, err == nil, c.want)
		}
	}
	// AckAll with everyone up works.
	g := newGroup(t, AckAll, "a", "b", "c")
	if err := write(g.rep(g.elect(1)), "x", 5*time.Second); err != nil {
		t.Fatal(err)
	}
}

func TestLaggingFollowerCatchesUpAfterLongOutage(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	leader := g.rep(g.elect(1))
	slow := "c"
	if leader.cfg.ID == "c" {
		slow = "b"
	}
	g.net.isolate(slow, true)
	for i := 0; i < 1500; i++ { // many batches of 256
		if err := write(leader, fmt.Sprint("w", i), 5*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if lag := leader.Info().Lag[slow]; lag < 1500 {
		t.Fatalf("lag for the isolated replica = %d, want >= 1500", lag)
	}
	g.net.isolate(slow, false)
	eventually(t, "lagging replica caught up", func() bool {
		return g.rep(slow).log.LastIndex() == leader.log.LastIndex() && leader.Info().Lag[slow] == 0
	})
}

func TestFailoverPreservesEveryCommittedWrite(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	old := g.elect(1)
	var acked []string
	for i := 0; i < 50; i++ {
		s := fmt.Sprint("w", i)
		if err := write(g.rep(old), s, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		acked = append(acked, s)
	}
	g.crash(old)
	next := g.elect(2)
	if next == old {
		t.Fatal("crashed node was elected")
	}
	got := contents(t, g.rep(next))
	if len(got) < len(acked) || fmt.Sprint(got[:len(acked)]) != fmt.Sprint(acked) {
		t.Fatalf("acknowledged writes lost in failover: have %d of %d", len(got), len(acked))
	}
	if err := write(g.rep(next), "after", 5*time.Second); err != nil {
		t.Fatal(err)
	}

	// The old node returns as a follower and converges.
	g.start(old)
	eventually(t, "old leader converged", func() bool { return g.rep(old).log.LastIndex() == g.rep(next).log.LastIndex() })
	if got := contents(t, g.rep(old)); fmt.Sprint(got) != fmt.Sprint(contents(t, g.rep(next))) {
		t.Fatal("restarted node diverged")
	}
}

// The core safety scenario: a leader cut off from its followers keeps
// accepting appends it can never commit. After a new leader is elected and
// the partition heals, the old leader must (1) learn it is deposed, (2)
// release its waiting writers with an unknown-outcome error, and (3) have its
// uncommitted entries replaced, not merged, by the new leader's history.
func TestDeposedLeaderIsFencedAndItsUncommittedEntriesDiscarded(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	old := g.elect(1)
	oldRep := g.rep(old)
	if err := write(oldRep, "committed-1", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	for _, id := range g.ids {
		id := id
		eventually(t, "replicated", func() bool { return g.rep(id).log.LastIndex() == oldRep.log.LastIndex() })
	}

	g.net.isolate(old, true) // network partition: old leader alone

	errc := make(chan error, 1)
	go func() { errc <- write(oldRep, "doomed", 10*time.Second) }() // can never reach a quorum
	eventually(t, "doomed write appended locally", func() bool { return oldRep.log.LastIndex() >= 3 })

	next := g.elect(2) // majority side (the two reachable replicas) elects a new leader
	if next == old {
		t.Fatal("isolated node won the election")
	}
	if err := write(g.rep(next), "committed-2", 5*time.Second); err != nil {
		t.Fatal(err)
	}

	g.net.isolate(old, false) // partition heals
	select {
	case err := <-errc:
		if !errors.Is(err, ErrLeadershipLost) {
			t.Fatalf("doomed write returned %v, want ErrLeadershipLost", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("deposed leader never released its writer")
	}
	eventually(t, "old leader stepped down", func() bool { return oldRep.Info().Role == Follower && g.steps[old].Load() >= 1 })
	if err := write(oldRep, "zombie", time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("write at the deposed leader = %v, want ErrNotLeader", err)
	}
	eventually(t, "old leader adopted the new history", func() bool {
		return fmt.Sprint(contents(t, oldRep)) == fmt.Sprint(contents(t, g.rep(next)))
	})
	for _, s := range contents(t, oldRep) {
		if s == "doomed" {
			t.Fatal("an entry that was never committed survived reconciliation")
		}
	}
}

func TestPromiseRejectsLowerEpochLeaders(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	var resp PrepareResp
	tr := g.net.transport("coord")
	tr.Call(context.Background(), "b", "Replica.Prepare", &PrepareReq{Key: "q/0", Epoch: 5}, &resp)
	var ar AppendResp
	tr.Call(context.Background(), "b", "Replica.AppendEntries", &AppendReq{Key: "q/0", Epoch: 3, Leader: "a"}, &ar)
	if ar.Success || ar.Epoch != 5 {
		t.Fatalf("lower-epoch append accepted or epoch not reported: %+v", ar)
	}
	tr.Call(context.Background(), "b", "Replica.Prepare", &PrepareReq{Key: "q/0", Epoch: 4}, &resp)
	if resp.OK || resp.Epoch != 5 {
		t.Fatalf("lower-epoch prepare accepted: %+v", resp)
	}
	tr.Call(context.Background(), "b", "Replica.Prepare", &PrepareReq{Key: "q/0", Epoch: 5}, &resp)
	if !resp.OK {
		t.Fatal("an idempotent retry of the same epoch must succeed")
	}
}

func TestLeaseExpiryStopsWrites(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	leader := g.rep(g.elect(1))
	leader.mu.Lock()
	leader.leaseUntil = g.clk.Now().Add(10 * time.Second)
	leader.mu.Unlock()
	if err := write(leader, "ok", 5*time.Second); err != nil {
		t.Fatal(err)
	}
	g.clk.Advance(11 * time.Second) // coordinator unreachable: no renewal
	if err := write(leader, "late", time.Second); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("write after lease expiry = %v, want ErrNotLeader", err)
	}
	lg, _ := leader.LeaderLog()
	if err := lg.(interface{ Err() error }).Err(); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("Err() = %v", err)
	}
	leader.SetLease(g.clk.Now().Add(10 * time.Second))
	if err := write(leader, "renewed", 5*time.Second); err != nil {
		t.Fatalf("write after renewal: %v", err)
	}
}

func TestCompactionIsRefused(t *testing.T) {
	g := newGroup(t, AckQuorum, "a", "b", "c")
	lg, _ := g.rep(g.elect(1)).LeaderLog()
	if _, err := lg.Rotate(); !errors.Is(err, ErrCompactionUnsupported) {
		t.Fatalf("Rotate = %v", err)
	}
}

// Property: under random crashes, restarts and elections, every write that was
// acknowledged at AckQuorum is present, in order, in the final leader's log.
func TestAcknowledgedWritesNeverLostUnderRandomFailovers(t *testing.T) {
	for seed := uint64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, 99))
			g := newGroup(t, AckQuorum, "a", "b", "c")
			epoch := uint64(1)
			leader := g.elect(epoch)
			var acked []string
			alive := map[string]bool{"a": true, "b": true, "c": true}
			for step := 0; step < 40; step++ {
				switch rng.IntN(6) {
				case 0, 1, 2, 3:
					s := fmt.Sprint("w", step)
					if err := write(g.rep(leader), s, 300*time.Millisecond); err == nil {
						acked = append(acked, s)
					}
				case 4: // crash a random live replica (never more than one down at a time)
					down := 0
					for _, a := range alive {
						if !a {
							down++
						}
					}
					if down == 0 {
						victim := g.ids[rng.IntN(3)]
						g.crash(victim)
						alive[victim] = false
						if victim == leader {
							epoch++
							leader = g.elect(epoch)
						}
					}
				case 5: // restart a dead replica
					for id, a := range alive {
						if !a {
							g.start(id)
							alive[id] = true
							break
						}
					}
				}
			}
			for id, a := range alive { // bring everyone back
				if !a {
					g.start(id)
				}
			}
			epoch++
			final := g.elect(epoch)
			got := contents(t, g.rep(final))
			// acked must be a subsequence prefix-preserving: all present in order.
			j := 0
			for _, e := range got {
				if j < len(acked) && e == acked[j] {
					j++
				}
			}
			if j != len(acked) {
				t.Fatalf("seed %d: %d of %d acknowledged writes survived\nacked=%v\nfinal=%v", seed, j, len(acked), acked, got)
			}
		})
	}
}
