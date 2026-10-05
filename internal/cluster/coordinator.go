package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

// CoordConfig configures a Coordinator.
type CoordConfig struct {
	Dir       string // state directory; "" keeps state in memory only
	Clock     clock.Clock
	Transport replication.Transport

	// FailureTimeout is how long a node may be silent before it is considered
	// failed. LeaseDuration is how long a leader may serve without renewing.
	// A failover cannot complete sooner than max(FailureTimeout, remaining
	// lease): the first is failure detection, the second is what bounds a
	// deposed leader's stale reads.
	FailureTimeout time.Duration
	LeaseDuration  time.Duration
	RPCTimeout     time.Duration
	DefaultRF      int
	Defaults       broker.QueueConfig
	Logger         *slog.Logger
}

type coordState struct {
	Version    uint64                `json:"version"`
	Queues     map[string]*QueueMeta `json:"queues"`
	GroupFloor map[string]uint64     `json:"group_floor"`
}

// Coordinator is the control plane: placement, failure detection, leader
// election and consumer-group coordination.
//
// IT IS A SINGLE PROCESS. This is the largest simplification in the
// repository. Replica logs are safe without it (epoch fencing and quorum
// commit are enforced by the replicas themselves), but while the coordinator
// is down no failover can happen, leases cannot be renewed, so leaders stop
// serving once their lease runs out, and group rebalances stop. A production
// system would run this role on a consensus group (etcd, or Raft built into
// the brokers) so that it has no single point of failure.
type Coordinator struct {
	cfg CoordConfig

	mu       sync.Mutex
	state    coordState
	lastSeen map[string]time.Time
	leaseEnd map[string]time.Time // per partition key, coordinator clock
	// leaderSince is when each current leader was recorded (coordinator clock).
	leaderSince                map[string]time.Time
	electing                   map[string]bool
	groups                     *groupTable
	elections, failedElections uint64
}

// NewCoordinator loads (or initialises) coordinator state.
func NewCoordinator(cfg CoordConfig) (*Coordinator, error) {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.FailureTimeout <= 0 {
		cfg.FailureTimeout = 3 * time.Second
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 5 * time.Second
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = time.Second
	}
	if cfg.DefaultRF <= 0 {
		cfg.DefaultRF = 3
	}
	if cfg.Logger == nil {
		cfg.Logger = observability.Discard()
	}
	cfg.Defaults = cfg.Defaults.WithDefaults()
	c := &Coordinator{cfg: cfg, lastSeen: map[string]time.Time{}, leaseEnd: map[string]time.Time{}, leaderSince: map[string]time.Time{},
		electing: map[string]bool{}, state: coordState{Queues: map[string]*QueueMeta{}, GroupFloor: map[string]uint64{}}}
	if cfg.Dir != "" {
		if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
			return nil, err
		}
		data, err := os.ReadFile(c.statePath())
		switch {
		case errors.Is(err, os.ErrNotExist):
		case err != nil:
			return nil, err
		default:
			if err := json.Unmarshal(data, &c.state); err != nil {
				return nil, fmt.Errorf("cluster: corrupt coordinator state: %w", err)
			}
			if c.state.Queues == nil {
				c.state.Queues = map[string]*QueueMeta{}
			}
			if c.state.GroupFloor == nil {
				c.state.GroupFloor = map[string]uint64{}
			}
		}
	}
	// After a restart the previous leases are unknown. Assume every recorded
	// leader holds a full lease starting now: an election cannot start before
	// the old leader's real lease has certainly expired.
	now := cfg.Clock.Now()
	for _, q := range c.state.Queues {
		for _, p := range q.Partitions {
			if p.Leader != "" {
				c.leaseEnd[Key(q.Name, p.Index)] = now.Add(cfg.LeaseDuration)
			}
		}
	}
	c.groups = newGroupTable(c.state.GroupFloor)
	return c, nil
}

func (c *Coordinator) statePath() string { return filepath.Join(c.cfg.Dir, "coordinator.json") }

// persistLocked writes state atomically (temp, fsync, rename, fsync dir).
func (c *Coordinator) persistLocked() error {
	c.state.Version++
	for k, v := range c.groups.floor {
		c.state.GroupFloor[k] = v
	}
	if c.cfg.Dir == "" {
		return nil
	}
	data, err := json.MarshalIndent(c.state, "", " ")
	if err != nil {
		return err
	}
	tmp := c.statePath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, werr := f.Write(data)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(tmp)
		return werr
	}
	if err := os.Rename(tmp, c.statePath()); err != nil {
		return err
	}
	d, err := os.Open(c.cfg.Dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func (c *Coordinator) aliveLocked(node string, now time.Time) bool {
	t, ok := c.lastSeen[node]
	return ok && now.Sub(t) <= c.cfg.FailureTimeout
}

func (c *Coordinator) aliveNodesLocked(now time.Time) []string {
	var out []string
	for n := range c.lastSeen {
		if c.aliveLocked(n, now) {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out
}

// ---- RPC surface ----

// Handler returns the RPC handler to register on the transport.
func (c *Coordinator) Handler() Handler {
	return func(ctx context.Context, from, method string, body json.RawMessage) (any, error) {
		switch method {
		case "Coord.Heartbeat":
			var r HeartbeatReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			return c.heartbeat(r), nil
		case "Coord.CreateQueue":
			var r CreateQueueReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			cfg, err := c.CreateQueue(r.Name, r.Config)
			return &QueueMeta{Name: r.Name, Config: cfg}, err
		case "Coord.DeleteQueue":
			var name string
			if err := json.Unmarshal(body, &name); err != nil {
				return nil, err
			}
			return struct{}{}, c.DeleteQueue(name)
		case "Coord.Metadata":
			return c.Metadata(), nil
		case "Coord.JoinGroup":
			var r JoinGroupReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			return c.joinGroup(r)
		case "Coord.GroupHeartbeat":
			var r GroupHeartbeatReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			return c.groupHeartbeat(r)
		case "Coord.LeaveGroup":
			var r GroupHeartbeatReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			c.mu.Lock()
			c.groups.Leave(r.Queue, r.Group, r.Member)
			err := c.persistLocked()
			c.mu.Unlock()
			return struct{}{}, err
		}
		return nil, fmt.Errorf("coordinator: unknown method %s", method)
	}
}

func (c *Coordinator) heartbeat(r HeartbeatReq) *HeartbeatResp {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.cfg.Clock.Now()
	c.lastSeen[r.Node] = now
	resp := &HeartbeatResp{Lease: map[string]time.Duration{}, Groups: c.groups.Generations()}

	led := map[string]uint64{}
	for _, l := range r.Led {
		led[l.Key] = l.Epoch
	}
	for _, q := range c.state.Queues {
		for _, p := range q.Partitions {
			key := Key(q.Name, p.Index)
			hosts := false
			for _, rep := range p.Replicas {
				hosts = hosts || rep == r.Node
			}
			if hosts {
				resp.Hosted = append(resp.Hosted, HostedPartition{Key: key, Queue: q.Name, Index: p.Index,
					Replicas: p.Replicas, Config: q.Config})
			}
			if p.Leader != r.Node {
				continue
			}
			if e, ok := led[key]; ok && e == p.Epoch {
				// The lease is granted from the moment the coordinator
				// RECEIVED this heartbeat, which is no earlier than when the
				// node sent it; the node counts its lease from its send time.
				// So the node's view of the lease always ends first.
				c.leaseEnd[key] = now.Add(c.cfg.LeaseDuration)
				resp.Lease[key] = c.cfg.LeaseDuration
			} else if !ok && now.Sub(c.leaderSince[key]) > 2*c.cfg.RPCTimeout {
				// The recorded leader says it no longer leads (it stepped
				// down or restarted). Clear it; an election follows once the
				// lease it held has certainly run out.
				//
				// The age check matters: a heartbeat built just before the
				// Promote RPC landed can arrive just after the coordinator
				// recorded the new leader, and rightly lacks the partition.
				// A heartbeat is in flight for at most RPCTimeout, so one
				// processed more than that after the leader was recorded was
				// necessarily built after the promotion and its omission is
				// real. Without the check every startup caused a spurious
				// second election.
				q.Partitions[p.Index].Leader = ""
				_ = c.persistLocked()
			}
		}
	}
	return resp
}

// CreateQueue places a new queue across the live nodes.
func (c *Coordinator) CreateQueue(name string, cfg broker.QueueConfig) (broker.QueueConfig, error) {
	if !broker.ValidName(name) {
		return cfg, fmt.Errorf("%w: invalid name", broker.ErrInvalidQueue)
	}
	cfg, err := cfg.Normalize(c.cfg.Defaults)
	if err != nil {
		return cfg, fmt.Errorf("%w: %v", broker.ErrInvalidQueue, err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.state.Queues[name]; ok {
		return cfg, broker.ErrQueueExists
	}
	nodes := c.aliveNodesLocked(c.cfg.Clock.Now())
	rf := cfg.ReplicationFactor
	if rf == 0 {
		rf = min(c.cfg.DefaultRF, len(nodes))
	}
	if rf < 1 || rf > len(nodes) {
		return cfg, fmt.Errorf("%w: replication_factor %d needs that many live nodes, have %d", broker.ErrInvalidQueue, rf, len(nodes))
	}
	cfg.ReplicationFactor = rf
	q := &QueueMeta{Name: name, Config: cfg}
	for i := 0; i < cfg.Partitions; i++ {
		reps := make([]string, rf)
		for j := range reps {
			reps[j] = nodes[(i+j)%len(nodes)] // rotate so leaders (replicas[0]) spread over nodes
		}
		q.Partitions = append(q.Partitions, PartitionMeta{Index: i, Replicas: reps})
	}
	c.state.Queues[name] = q
	if err := c.persistLocked(); err != nil {
		delete(c.state.Queues, name)
		return cfg, err
	}
	c.cfg.Logger.Info("queue created", "queue", name, "partitions", cfg.Partitions, "rf", rf)
	return cfg, nil
}

// DeleteQueue removes a queue; nodes drop its replicas on their next heartbeat.
func (c *Coordinator) DeleteQueue(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.state.Queues[name]; !ok {
		return broker.ErrQueueNotFound
	}
	delete(c.state.Queues, name)
	for k := range c.groups.groups {
		if q, _ := splitGroupKey(k); q == name {
			delete(c.groups.groups, k)
		}
	}
	return c.persistLocked()
}

// Metadata returns a deep copy of cluster metadata.
func (c *Coordinator) Metadata() *Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	m := &Metadata{Version: c.state.Version, Queues: map[string]*QueueMeta{}, Alive: c.aliveNodesLocked(c.cfg.Clock.Now())}
	for n, q := range c.state.Queues {
		cp := *q
		cp.Partitions = make([]PartitionMeta, len(q.Partitions))
		for i, p := range q.Partitions {
			p.Replicas = append([]string(nil), p.Replicas...)
			cp.Partitions[i] = p
		}
		m.Queues[n] = &cp
	}
	return m
}

func (c *Coordinator) partitionsOf(queue string) (int, error) {
	q := c.state.Queues[queue]
	if q == nil {
		return 0, broker.ErrQueueNotFound
	}
	return len(q.Partitions), nil
}

func (c *Coordinator) joinGroup(r JoinGroupReq) (*GroupResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, err := c.partitionsOf(r.Queue)
	if err != nil {
		return nil, err
	}
	if r.SessionTimeout <= 0 {
		r.SessionTimeout = 10 * time.Second
	}
	gen, a := c.groups.Join(r.Queue, r.Group, r.Member, n, r.SessionTimeout, c.cfg.Clock.Now())
	if err := c.persistLocked(); err != nil {
		return nil, err
	}
	return &GroupResp{Generation: gen, Assigned: a}, nil
}

func (c *Coordinator) groupHeartbeat(r GroupHeartbeatReq) (*GroupResp, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	gen, a, ok := c.groups.Heartbeat(r.Queue, r.Group, r.Member, c.cfg.Clock.Now())
	if !ok {
		return nil, ErrUnknownMember
	}
	return &GroupResp{Generation: gen, Assigned: a}, nil
}

// Groups returns every consumer group's current assignment.
func (c *Coordinator) Groups() []GroupView {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groups.Views()
}

// Stats is a snapshot for metrics and tests.
type CoordStats struct {
	AliveNodes      []string
	Elections       uint64
	FailedElections uint64
	Rebalances      uint64
}

// Stats returns counters.
func (c *Coordinator) Stats() CoordStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CoordStats{AliveNodes: c.aliveNodesLocked(c.cfg.Clock.Now()), Elections: c.elections,
		FailedElections: c.failedElections, Rebalances: c.groups.Rebalances}
}

// ---- failure detection and elections ----

// Tick performs one round of control-plane work: expire silent group members
// and start an election for every partition that needs a leader. Elections
// run concurrently and Tick returns when they have finished.
func (c *Coordinator) Tick(ctx context.Context) {
	c.mu.Lock()
	now := c.cfg.Clock.Now()
	if removed := c.groups.Expire(now); len(removed) > 0 {
		c.cfg.Logger.Info("group members expired", "members", removed)
		_ = c.persistLocked()
	}
	type job struct {
		key      string
		replicas []string
		epoch    uint64
	}
	var todo []job
	for _, q := range c.state.Queues {
		for i := range q.Partitions {
			p := &q.Partitions[i]
			key := Key(q.Name, p.Index)
			if c.electing[key] {
				continue
			}
			leaderOK := p.Leader != "" && c.aliveLocked(p.Leader, now)
			if leaderOK {
				continue
			}
			if now.Before(c.leaseEnd[key]) {
				continue // the previous leader's lease may still be valid; wait it out
			}
			p.Leader = ""
			p.Epoch++ // persisted BEFORE any Prepare, so an epoch is never issued twice
			c.electing[key] = true
			todo = append(todo, job{key, append([]string(nil), p.Replicas...), p.Epoch})
		}
	}
	if len(todo) > 0 {
		_ = c.persistLocked()
	}
	c.mu.Unlock()

	var wg sync.WaitGroup
	for _, j := range todo {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.elect(ctx, j.key, j.replicas, j.epoch)
		}()
	}
	wg.Wait()
}

// elect runs one election for partition key at epoch.
//
//  1. Ask every replica to Prepare (durably promise) the epoch. A replica
//     that promised rejects the previous leader from then on, so after a
//     majority has promised, the old leader can no longer commit anything.
//  2. Require a majority of promises. Any entry that was committed is on a
//     majority, and two majorities intersect, so at least one promising
//     replica holds every committed entry.
//  3. Promote the promising replica with the most up-to-date log: highest
//     last epoch, then longest. It holds every committed entry.
//  4. Record the leader and start its lease.
func (c *Coordinator) elect(ctx context.Context, key string, replicas []string, epoch uint64) {
	defer func() {
		c.mu.Lock()
		delete(c.electing, key)
		c.mu.Unlock()
	}()
	type pos struct {
		node         string
		order        int
		lastE, lastI uint64
	}
	var (
		mu   sync.Mutex
		oks  []pos
		wg   sync.WaitGroup
		tr   = c.cfg.Transport
		fail = func(why string, err error) {
			c.mu.Lock()
			c.failedElections++
			c.mu.Unlock()
			c.cfg.Logger.Warn("election failed", "partition", key, "epoch", epoch, "why", why, "err", err)
		}
	)
	for i, n := range replicas {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
			defer cancel()
			var resp replication.PrepareResp
			err := tr.Call(cctx, n, "Replica.Prepare", &replication.PrepareReq{Key: key, Epoch: epoch}, &resp)
			if err == nil && resp.OK {
				mu.Lock()
				oks = append(oks, pos{n, i, resp.LastEpoch, resp.LastIndex})
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(oks) < len(replicas)/2+1 {
		fail("no majority promised", ErrNoQuorum)
		return
	}
	best := oks[0]
	for _, p := range oks[1:] {
		switch {
		case p.lastE != best.lastE:
			if p.lastE > best.lastE {
				best = p
			}
		case p.lastI != best.lastI:
			if p.lastI > best.lastI {
				best = p
			}
		case p.order < best.order: // equal logs: prefer the placement-preferred replica
			best = p
		}
	}
	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	err := tr.Call(cctx, best.node, "Node.Promote",
		&PromoteReq{Key: key, Epoch: epoch, Replicas: replicas, LeaseFor: c.cfg.LeaseDuration}, nil)
	if err != nil {
		fail("promote "+best.node, err)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	qn, idx := splitKey(key)
	if q := c.state.Queues[qn]; q != nil && idx < len(q.Partitions) && q.Partitions[idx].Epoch == epoch {
		q.Partitions[idx].Leader = best.node
		c.leaseEnd[key] = c.cfg.Clock.Now().Add(c.cfg.LeaseDuration) // after the node's own start
		c.leaderSince[key] = c.cfg.Clock.Now()
		c.elections++
		_ = c.persistLocked()
		c.cfg.Logger.Info("leader elected", "partition", key, "leader", best.node, "epoch", epoch,
			"last_epoch", best.lastE, "last_index", best.lastI)
	}
}

func splitKey(k string) (queue string, idx int) {
	for i := len(k) - 1; i >= 0; i-- {
		if k[i] == '/' {
			fmt.Sscanf(k[i+1:], "%d", &idx)
			return k[:i], idx
		}
	}
	return k, 0
}

// Run drives Tick on a timer until ctx is done.
func (c *Coordinator) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Tick(ctx)
		}
	}
}
