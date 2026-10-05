package cluster

import (
	"sort"
	"time"
)

// Consumer groups: a set of workers that share a queue's partitions.
//
// Why groups exist in a queue where leases already make each job exclusive:
// without them every worker polls every partition, which causes wasted polls
// and thundering herds, spreads a partition key's jobs across all workers,
// and gives no way to say "this worker owns partition 3". Groups give each
// partition one owner at a time, balanced across live workers, with the
// owner changing only on membership change.
//
// Rebalancing strategy: eager, centrally computed, sticky.
//   - Eager: any membership change bumps the generation and recomputes the
//     whole assignment at once. Members learn the new generation and
//     assignment on their next heartbeat and switch together. (Kafka's
//     cooperative protocol moves partitions incrementally to avoid a
//     stop-the-world pause; with a central computation that cost is just
//     one heartbeat interval, so the simpler protocol is kept.)
//   - Sticky: a partition stays with its current owner unless balance
//     requires moving it, so a join or leave moves the minimum number of
//     partitions.
//   - Generation fencing: every pull carries the member's generation and a
//     node rejects generations older than the one it knows, so a member
//     that missed a rebalance cannot keep consuming under a stale
//     assignment for longer than a heartbeat.
//
// Job-level leases remain the final guard: even if two members briefly
// overlap on a partition during a rebalance they cannot hold the same job.

// groupAssign computes a balanced, sticky assignment of partitions
// [0, parts) to members. Member order is by name, so the result is
// deterministic. Sizes differ by at most one.
func groupAssign(members []string, parts int, prev map[string][]int) map[string][]int {
	out := make(map[string][]int, len(members))
	if len(members) == 0 {
		return out
	}
	sorted := append([]string(nil), members...)
	sort.Strings(sorted)
	base, extra := parts/len(sorted), parts%len(sorted)

	// Members that already hold more partitions get the "extra" slots first,
	// which minimises movement; ties go to the lexically first names.
	order := append([]string(nil), sorted...)
	sort.SliceStable(order, func(i, j int) bool { return len(prev[order[i]]) > len(prev[order[j]]) })
	capacity := map[string]int{}
	for i, m := range order {
		capacity[m] = base
		if i < extra {
			capacity[m]++
		}
	}

	owned := make([]bool, parts)
	for _, m := range sorted {
		kept := 0
		for _, p := range prev[m] {
			if p >= 0 && p < parts && !owned[p] && kept < capacity[m] {
				owned[p] = true
				out[m] = append(out[m], p)
				kept++
			}
		}
	}
	var free []int
	for p := 0; p < parts; p++ {
		if !owned[p] {
			free = append(free, p)
		}
	}
	for _, m := range sorted {
		for len(out[m]) < capacity[m] && len(free) > 0 {
			out[m] = append(out[m], free[0])
			free = free[1:]
		}
	}
	for m := range out {
		sort.Ints(out[m])
	}
	for _, m := range sorted {
		if out[m] == nil {
			out[m] = []int{}
		}
	}
	return out
}

type groupMember struct {
	lastBeat time.Time
	session  time.Duration
}

type group struct {
	generation uint64
	members    map[string]*groupMember
	assign     map[string][]int
	partitions int
}

// groupTable is the coordinator's view of every consumer group. It is not
// internally synchronised: the Coordinator holds its lock around calls.
type groupTable struct {
	groups map[string]*group
	// floor remembers the highest generation ever issued per group so a
	// restarted coordinator never reuses one (reuse would defeat fencing).
	floor map[string]uint64
	// Rebalances counts generation changes, for metrics.
	Rebalances uint64
}

func newGroupTable(floor map[string]uint64) *groupTable {
	if floor == nil {
		floor = map[string]uint64{}
	}
	return &groupTable{groups: map[string]*group{}, floor: floor}
}

func groupKey(queue, name string) string { return queue + "/" + name }

func (t *groupTable) rebalance(k string, g *group) {
	members := make([]string, 0, len(g.members))
	for m := range g.members {
		members = append(members, m)
	}
	g.generation = max(g.generation, t.floor[k]) + 1
	t.floor[k] = g.generation
	g.assign = groupAssign(members, g.partitions, g.assign)
	t.Rebalances++
}

// Join adds or refreshes a member and returns the group's generation and the
// member's partitions. A new member triggers a rebalance; a known member
// rejoining without change does not.
func (t *groupTable) Join(queue, name, member string, partitions int, session time.Duration, now time.Time) (uint64, []int) {
	k := groupKey(queue, name)
	g := t.groups[k]
	if g == nil {
		g = &group{members: map[string]*groupMember{}, assign: map[string][]int{}, partitions: partitions}
		t.groups[k] = g
	}
	g.partitions = partitions
	_, known := g.members[member]
	g.members[member] = &groupMember{lastBeat: now, session: session}
	if !known {
		t.rebalance(k, g)
	}
	return g.generation, append([]int(nil), g.assign[member]...)
}

// Heartbeat refreshes a member. It returns the current generation and
// assignment; the member compares the generation with its own to learn
// whether a rebalance happened. ok is false for an unknown member (it was
// expired or the coordinator restarted) which must rejoin.
func (t *groupTable) Heartbeat(queue, name, member string, now time.Time) (gen uint64, assigned []int, ok bool) {
	g := t.groups[groupKey(queue, name)]
	if g == nil {
		return 0, nil, false
	}
	m := g.members[member]
	if m == nil {
		return g.generation, nil, false
	}
	m.lastBeat = now
	return g.generation, append([]int(nil), g.assign[member]...), true
}

// Leave removes a member immediately (graceful shutdown), which is faster than
// waiting for its session to time out.
func (t *groupTable) Leave(queue, name, member string) {
	k := groupKey(queue, name)
	if g := t.groups[k]; g != nil {
		if _, ok := g.members[member]; ok {
			delete(g.members, member)
			t.rebalance(k, g)
		}
	}
}

// Expire removes members whose session lapsed and rebalances affected groups.
// It returns the members removed, as "queue/group/member".
func (t *groupTable) Expire(now time.Time) []string {
	var removed []string
	for k, g := range t.groups {
		changed := false
		for m, st := range g.members {
			if now.Sub(st.lastBeat) > st.session {
				delete(g.members, m)
				removed = append(removed, k+"/"+m)
				changed = true
			}
		}
		if changed {
			t.rebalance(k, g)
		}
	}
	sort.Strings(removed)
	return removed
}

// Generations lists the current generation of every group, for nodes.
func (t *groupTable) Generations() map[string]uint64 {
	out := make(map[string]uint64, len(t.groups))
	for k, g := range t.groups {
		out[k] = g.generation
	}
	return out
}

// View describes a group for inspection.
type GroupView struct {
	Queue      string           `json:"queue"`
	Group      string           `json:"group"`
	Generation uint64           `json:"generation"`
	Assignment map[string][]int `json:"assignment"`
}

// Views returns every group's assignment.
func (t *groupTable) Views() []GroupView {
	var out []GroupView
	for k, g := range t.groups {
		queue, name := splitGroupKey(k)
		a := map[string][]int{}
		for m, p := range g.assign {
			a[m] = append([]int(nil), p...)
		}
		out = append(out, GroupView{Queue: queue, Group: name, Generation: g.generation, Assignment: a})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Queue+out[i].Group < out[j].Queue+out[j].Group })
	return out
}

func splitGroupKey(k string) (queue, name string) {
	for i := 0; i < len(k); i++ {
		if k[i] == '/' {
			return k[:i], k[i+1:]
		}
	}
	return k, ""
}
