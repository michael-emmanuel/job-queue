package cluster

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"testing"
	"time"
)

func checkAssignment(t *testing.T, a map[string][]int, members []string, parts int) {
	t.Helper()
	seen := map[int]string{}
	minN, maxN := parts+1, -1
	for _, m := range members {
		ps, ok := a[m]
		if !ok {
			t.Fatalf("member %s missing from assignment", m)
		}
		minN, maxN = min(minN, len(ps)), max(maxN, len(ps))
		for _, p := range ps {
			if p < 0 || p >= parts {
				t.Fatalf("partition %d out of range", p)
			}
			if o, dup := seen[p]; dup {
				t.Fatalf("partition %d assigned to both %s and %s", p, o, m)
			}
			seen[p] = m
		}
	}
	if len(seen) != parts {
		t.Fatalf("%d of %d partitions assigned", len(seen), parts)
	}
	if len(members) > 0 && maxN-minN > 1 {
		t.Fatalf("unbalanced: sizes range %d..%d", minN, maxN)
	}
}

func TestSixPartitionsThreeWorkers(t *testing.T) {
	a := groupAssign([]string{"worker-3", "worker-1", "worker-2"}, 6, nil)
	want := map[string][]int{"worker-1": {0, 1}, "worker-2": {2, 3}, "worker-3": {4, 5}}
	if fmt.Sprint(a) != fmt.Sprint(want) {
		t.Fatalf("got %v want %v", a, want)
	}
}

func TestWorkerDeathMovesOnlyItsPartitions(t *testing.T) {
	prev := groupAssign([]string{"w1", "w2", "w3"}, 6, nil)
	after := groupAssign([]string{"w1", "w3"}, 6, prev)
	checkAssignment(t, after, []string{"w1", "w3"}, 6)
	for _, m := range []string{"w1", "w3"} {
		for _, p := range prev[m] {
			found := false
			for _, q := range after[m] {
				found = found || p == q
			}
			if !found {
				t.Errorf("%s lost partition %d it already owned: not sticky", m, p)
			}
		}
	}
	moved := 0
	for _, p := range prev["w2"] {
		_ = p
		moved++
	}
	if moved != 2 {
		t.Fatalf("expected the dead worker's 2 partitions to move, got %d", moved)
	}
}

func TestJoinMovesMinimumPartitions(t *testing.T) {
	prev := groupAssign([]string{"w1", "w2"}, 8, nil)
	after := groupAssign([]string{"w1", "w2", "w3"}, 8, prev)
	checkAssignment(t, after, []string{"w1", "w2", "w3"}, 8)
	owner := func(a map[string][]int) map[int]string {
		o := map[int]string{}
		for m, ps := range a {
			for _, p := range ps {
				o[p] = m
			}
		}
		return o
	}
	po, ao := owner(prev), owner(after)
	moved := 0
	for p := 0; p < 8; p++ {
		if po[p] != ao[p] {
			moved++
		}
	}
	if moved != len(after["w3"]) {
		t.Fatalf("%d partitions moved but the new member only took %d: partitions moved between existing members", moved, len(after["w3"]))
	}
}

func TestMoreMembersThanPartitions(t *testing.T) {
	a := groupAssign([]string{"a", "b", "c", "d", "e"}, 3, nil)
	checkAssignment(t, a, []string{"a", "b", "c", "d", "e"}, 3)
	idle := 0
	for _, ps := range a {
		if len(ps) == 0 {
			idle++
		}
	}
	if idle != 2 {
		t.Fatalf("%d idle members, want 2", idle)
	}
}

func TestAssignmentProperties(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 7))
	prev := map[string][]int{}
	for i := 0; i < 500; i++ {
		parts := 1 + rng.IntN(24)
		n := 1 + rng.IntN(10)
		var members []string
		for _, k := range rng.Perm(12)[:n] {
			members = append(members, fmt.Sprint("m", k))
		}
		a := groupAssign(members, parts, prev)
		checkAssignment(t, a, members, parts)
		b := groupAssign(members, parts, prev)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatal("assignment is not deterministic")
		}
		prev = a
	}
}

func TestGroupTableLifecycle(t *testing.T) {
	now := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	gt := newGroupTable(nil)
	g1, a1 := gt.Join("q", "g", "w1", 4, 10*time.Second, now)
	if g1 != 1 || len(a1) != 4 {
		t.Fatalf("first member: gen=%d assigned=%v", g1, a1)
	}
	g2, a2 := gt.Join("q", "g", "w2", 4, 10*time.Second, now)
	if g2 != 2 || len(a2) != 2 {
		t.Fatalf("second member: gen=%d assigned=%v", g2, a2)
	}
	// w1 learns of the rebalance on its next heartbeat.
	gen, a, ok := gt.Heartbeat("q", "g", "w1", now.Add(time.Second))
	if !ok || gen != 2 || len(a) != 2 {
		t.Fatalf("heartbeat: %d %v %v", gen, a, ok)
	}
	// Rejoining without change must not churn the generation.
	if g, _ := gt.Join("q", "g", "w2", 4, 10*time.Second, now.Add(time.Second)); g != 2 {
		t.Fatalf("idempotent rejoin bumped generation to %d", g)
	}

	// w2 stops heartbeating; w1 keeps going. After the session lapses w2's
	// partitions move to w1.
	gt.Heartbeat("q", "g", "w1", now.Add(8*time.Second))
	removed := gt.Expire(now.Add(12 * time.Second))
	if len(removed) != 1 || removed[0] != "q/g/w2" {
		t.Fatalf("removed %v", removed)
	}
	gen, a, ok = gt.Heartbeat("q", "g", "w1", now.Add(12*time.Second))
	if !ok || gen != 3 || len(a) != 4 {
		t.Fatalf("after expiry: gen=%d assigned=%v", gen, a)
	}
	if _, _, ok := gt.Heartbeat("q", "g", "w2", now.Add(13*time.Second)); ok {
		t.Fatal("expired member must be told to rejoin")
	}

	gt.Leave("q", "g", "w1")
	if v := gt.Views(); len(v) != 1 || len(v[0].Assignment) != 0 {
		t.Fatalf("views after last leave: %+v", v)
	}
}

func TestGenerationNeverReusedAfterCoordinatorRestart(t *testing.T) {
	now := time.Now()
	gt := newGroupTable(nil)
	gt.Join("q", "g", "a", 2, time.Minute, now)
	gt.Join("q", "g", "b", 2, time.Minute, now)
	floor := map[string]uint64{}
	for k, v := range gt.floor {
		floor[k] = v
	}
	restarted := newGroupTable(floor) // group state lost, floor persisted
	gen, _ := restarted.Join("q", "g", "a", 2, time.Minute, now)
	if gen <= 2 {
		t.Fatalf("generation %d reused after restart; a stale member's generation would look current", gen)
	}
}

func TestGenerationsAreMonotonicUnderChurn(t *testing.T) {
	now := time.Now()
	gt := newGroupTable(nil)
	var last uint64
	rng := rand.New(rand.NewPCG(3, 3))
	live := map[string]bool{}
	for i := 0; i < 300; i++ {
		m := fmt.Sprint("m", rng.IntN(6))
		if live[m] && rng.IntN(2) == 0 {
			gt.Leave("q", "g", m)
			delete(live, m)
		} else {
			gt.Join("q", "g", m, 7, time.Minute, now)
			live[m] = true
		}
		if v := gt.Views(); len(v) == 1 {
			if v[0].Generation < last {
				t.Fatalf("generation went backwards: %d -> %d", last, v[0].Generation)
			}
			last = v[0].Generation
			var members []string
			for k := range live {
				members = append(members, k)
			}
			sort.Strings(members)
			if len(members) > 0 {
				checkAssignment(t, v[0].Assignment, members, 7)
			}
		}
	}
}
