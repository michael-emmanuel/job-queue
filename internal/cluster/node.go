package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

// NodeConfig configures a Node.
type NodeConfig struct {
	Name       string
	Dir        string
	Clock      clock.Clock
	Transport  replication.Transport
	Coord      string        // coordinator endpoint name
	RPCTimeout time.Duration // default 1s
	// ReplHeartbeat is the idle replication heartbeat in real time (default 50ms).
	ReplHeartbeat time.Duration
	Logger        *slog.Logger
}

// Node hosts replicas of the partitions the coordinator assigns to it and
// serves the leader side of those it leads.
//
// A partition on a node is in one of two states. As a follower it is just a
// replicated log: no in-memory queue state, so a follower costs disk and
// replication bandwidth but little memory. On becoming leader the node
// rebuilds the queue state by replaying the log (failover time grows with
// log length until compaction exists for replicated partitions), and on
// losing leadership it throws that state away and returns to being a
// follower. Discarding rather than repairing is deliberate: the leader applies
// writes speculatively before they commit, so after a deposition its memory
// may contain effects of entries the new leader never had.
type Node struct {
	cfg NodeConfig

	mu       sync.Mutex
	hosted   map[string]*hostedPartition
	groupGen map[string]uint64
	closed   bool
	wg       sync.WaitGroup
	lastOK   time.Time // last successful coordinator heartbeat (wall clock)
}

type hostedPartition struct {
	meta    HostedPartition
	replica *replication.Replica

	mu     sync.Mutex // guards part, cancel
	part   *partition.Partition
	cancel context.CancelFunc
}

// NewNode creates a node. Call Tick periodically (or Run) and register
// Handler on the transport.
func NewNode(cfg NodeConfig) *Node {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = time.Second
	}
	if cfg.ReplHeartbeat <= 0 {
		cfg.ReplHeartbeat = 50 * time.Millisecond
	}
	if cfg.Logger == nil {
		cfg.Logger = observability.Discard()
	}
	cfg.Logger = cfg.Logger.With("node", cfg.Name)
	return &Node{cfg: cfg, hosted: map[string]*hostedPartition{}, groupGen: map[string]uint64{}}
}

// Name returns the node's endpoint name.
func (n *Node) Name() string { return n.cfg.Name }

func (n *Node) replicaPath(queue string, idx int) string {
	return filepath.Join(n.cfg.Dir, "queues", queue, fmt.Sprintf("p%03d", idx), "rlog")
}

func ackLevel(s string) replication.AckLevel {
	switch s {
	case "leader":
		return replication.AckLeader
	case "all":
		return replication.AckAll
	}
	return replication.AckQuorum
}

// host starts keeping a replica of a partition.
func (n *Node) host(hp HostedPartition) error {
	path := n.replicaPath(hp.Queue, hp.Index)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	h := &hostedPartition{meta: hp}
	r, err := replication.OpenReplica(replication.Config{
		ID: n.cfg.Name, Key: hp.Key, Path: path, Clock: n.cfg.Clock, Transport: n.cfg.Transport,
		Ack: ackLevel(hp.Config.Ack), Heartbeat: n.cfg.ReplHeartbeat, RPCTimeout: n.cfg.RPCTimeout,
		Logger: n.cfg.Logger,
		OnStepDown: func(epoch uint64) {
			n.cfg.Logger.Warn("deposed", "partition", hp.Key, "new_epoch", epoch)
			n.dropPartition(h)
		},
	})
	if err != nil {
		return err
	}
	h.replica = r
	n.hosted[hp.Key] = h
	n.cfg.Logger.Info("hosting replica", "partition", hp.Key, "replicas", hp.Replicas)
	return nil
}

// dropPartition discards the in-memory queue state. It is idempotent.
func (n *Node) dropPartition(h *hostedPartition) {
	h.mu.Lock()
	p, cancel := h.part, h.cancel
	h.part, h.cancel = nil, nil
	h.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if p != nil {
		p.Close()
	}
}

func (n *Node) unhost(key string) {
	h := n.hosted[key]
	delete(n.hosted, key)
	if h == nil {
		return
	}
	h.replica.StopLeading()
	n.dropPartition(h)
	h.replica.Close()
	if err := os.RemoveAll(filepath.Dir(n.replicaPath(h.meta.Queue, h.meta.Index))); err != nil {
		n.cfg.Logger.Error("removing replica data", "partition", key, "err", err)
	}
}

// promote makes this node the leader of a partition. The replica must already
// have promised the epoch (the coordinator's Prepare did that).
func (n *Node) promote(req PromoteReq) error {
	n.mu.Lock()
	h := n.hosted[req.Key]
	n.mu.Unlock()
	if h == nil {
		return ErrNotHosted
	}
	until := n.cfg.Clock.Now().Add(req.LeaseFor)
	if err := h.replica.StartLeading(req.Epoch, req.Replicas, until); err != nil {
		return err
	}
	lg, err := h.replica.LeaderLog()
	if err == nil {
		var pc partition.Config
		if pc, err = h.meta.Config.PartitionConfig(h.meta.Queue, h.meta.Index); err == nil {
			pc.Log, pc.Clock = lg, n.cfg.Clock
			pc.Logger = n.cfg.Logger.With("queue", h.meta.Queue)
			var p *partition.Partition
			if p, err = partition.Open(pc); err == nil {
				ctx, cancel := context.WithCancel(context.Background())
				h.mu.Lock()
				h.part, h.cancel = p, cancel
				h.mu.Unlock()
				n.wg.Add(1)
				go func() { defer n.wg.Done(); p.Run(ctx) }()
				n.cfg.Logger.Info("leading", "partition", req.Key, "epoch", req.Epoch,
					"ready", p.Stats().Ready, "in_flight", p.Stats().InFlight)
				return nil
			}
		}
	}
	h.replica.StopLeading()
	return err
}

// Tick sends a heartbeat, applies the coordinator's answer, and demotes any
// partition whose lease has expired. Call it periodically.
func (n *Node) Tick(ctx context.Context) {
	sentAt := n.cfg.Clock.Now()
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return
	}
	var led []LedPartition
	for k, h := range n.hosted {
		if in := h.replica.Info(); in.Role == replication.Leader {
			led = append(led, LedPartition{Key: k, Epoch: in.Epoch})
		}
	}
	n.mu.Unlock()
	sort.Slice(led, func(i, j int) bool { return led[i].Key < led[j].Key })

	cctx, cancel := context.WithTimeout(ctx, n.cfg.RPCTimeout)
	var resp HeartbeatResp
	err := n.cfg.Transport.Call(cctx, n.cfg.Coord, "Coord.Heartbeat", &HeartbeatReq{Node: n.cfg.Name, Led: led}, &resp)
	cancel()

	n.mu.Lock()
	if err == nil {
		n.lastOK = time.Now()
	}
	if err == nil && !n.closed {
		want := map[string]bool{}
		for _, hp := range resp.Hosted {
			want[hp.Key] = true
			if n.hosted[hp.Key] == nil {
				if herr := n.host(hp); herr != nil {
					n.cfg.Logger.Error("cannot host replica", "partition", hp.Key, "err", herr)
				}
			}
		}
		for k := range n.hosted {
			if !want[k] {
				n.unhost(k)
			}
		}
		n.groupGen = resp.Groups
		for _, l := range led {
			h := n.hosted[l.Key]
			if h == nil {
				continue
			}
			if d, ok := resp.Lease[l.Key]; ok {
				h.replica.SetLease(sentAt.Add(d))
			} else {
				// The coordinator no longer considers us the leader.
				n.cfg.Logger.Warn("leadership revoked by coordinator", "partition", l.Key)
				h.replica.StopLeading()
				n.dropPartition(h)
			}
		}
	}
	// Lease expiry is enforced locally even if the coordinator is unreachable:
	// a leader that cannot renew must stop, because a new leader may be
	// elected once the lease has run out.
	now := n.cfg.Clock.Now()
	for k, h := range n.hosted {
		if in := h.replica.Info(); in.Role == replication.Leader && !now.Before(in.LeaseUntil) {
			n.cfg.Logger.Warn("lease expired; stepping down", "partition", k)
			h.replica.StopLeading()
			n.dropPartition(h)
		}
	}
	n.mu.Unlock()
}

// Run drives Tick until ctx is done.
func (n *Node) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			n.Tick(ctx)
		}
	}
}

// Close stops all partitions and closes replica logs.
func (n *Node) Close() error {
	n.mu.Lock()
	n.closed = true
	hs := make([]*hostedPartition, 0, len(n.hosted))
	for _, h := range n.hosted {
		hs = append(hs, h)
	}
	n.mu.Unlock()
	for _, h := range hs {
		h.replica.StopLeading()
		n.dropPartition(h)
		h.replica.Close()
	}
	n.wg.Wait()
	return nil
}

// ReplicaStatus describes one hosted partition.
type ReplicaStatus struct {
	Key  string
	Info replication.Info
	// QueueStats is set only while this node leads the partition.
	QueueStats *partition.Stats
}

// Status lists hosted partitions.
func (n *Node) Status() []ReplicaStatus {
	n.mu.Lock()
	defer n.mu.Unlock()
	var out []ReplicaStatus
	for k, h := range n.hosted {
		st := ReplicaStatus{Key: k, Info: h.replica.Info()}
		h.mu.Lock()
		if h.part != nil {
			s := h.part.Stats()
			st.QueueStats = &s
		}
		h.mu.Unlock()
		out = append(out, st)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// ---- RPC handler ----

// Handler returns the RPC handler to register on the transport.
func (n *Node) Handler() Handler {
	return func(ctx context.Context, from, method string, body json.RawMessage) (any, error) {
		switch method {
		case "Replica.AppendEntries":
			var r replication.AppendReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			h, err := n.get(r.Key)
			if err != nil {
				return nil, err
			}
			return h.replica.HandleAppend(ctx, &r)
		case "Replica.Prepare":
			var r replication.PrepareReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			h, err := n.get(r.Key)
			if err != nil {
				return nil, err
			}
			resp, err := h.replica.HandlePrepare(&r)
			if err == nil && resp.OK {
				n.dropPartition(h) // promised a newer epoch: any local leadership is over
			}
			return resp, err
		case "Node.Promote":
			var r PromoteReq
			if err := json.Unmarshal(body, &r); err != nil {
				return nil, err
			}
			return struct{}{}, n.promote(r)
		}
		return n.clientOp(ctx, method, body)
	}
}

func (n *Node) get(key string) (*hostedPartition, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	h := n.hosted[key]
	if h == nil {
		return nil, ErrNotHosted
	}
	return h, nil
}

// leader returns the partition engine if (and only if) this node currently
// leads it with a valid lease.
func (n *Node) leader(key string) (*partition.Partition, error) {
	h, err := n.get(key)
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	p := h.part
	h.mu.Unlock()
	if p == nil || p.Healthy() != nil {
		return nil, notLeader(h.replica.Info().LeaderID)
	}
	return p, nil
}

func (n *Node) clientOp(ctx context.Context, method string, body json.RawMessage) (any, error) {
	dec := func(v any) error { return json.Unmarshal(body, v) }
	switch method {
	case "Node.Enqueue":
		var r EnqueueReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		res, err := p.Enqueue(ctx, r.Job)
		return &EnqueueResp{ID: res.ID, Duplicate: res.Duplicate}, err
	case "Node.Pull":
		var r PullReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		if r.Group != "" {
			queue, _ := splitKey(r.Key)
			n.mu.Lock()
			cur := n.groupGen[groupKey(queue, r.Group)]
			n.mu.Unlock()
			if r.Generation < cur {
				return nil, ErrStaleGeneration
			}
		}
		jobs, err := p.Dequeue(ctx, partition.DequeueRequest{Worker: r.Worker, Max: r.Max, Visibility: r.Visibility, Wait: r.Wait})
		return &PullResp{Jobs: jobs}, err
	case "Node.Ack":
		var r AckReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		return struct{}{}, p.Ack(ctx, r.ID, r.Attempt)
	case "Node.Fail":
		var r FailReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		out, err := p.Fail(ctx, r.ID, r.Attempt, partition.Failure{Err: r.Err, NonRetryable: r.NonRetryable})
		return &FailResp{Outcome: string(out)}, err
	case "Node.Renew":
		var r RenewReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		dl, err := p.Renew(ctx, r.ID, r.Attempt, r.Extend)
		return &RenewResp{Deadline: dl}, err
	case "Node.Get":
		var r JobReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		j, err := p.Get(r.ID)
		return &JobResp{Job: j}, err
	case "Node.Cancel", "Node.ReplayDead", "Node.DeleteDead":
		var r JobReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		switch method {
		case "Node.Cancel":
			err = p.Cancel(ctx, r.ID)
		case "Node.ReplayDead":
			err = p.ReplayDead(ctx, r.ID)
		default:
			err = p.DeleteDead(ctx, r.ID)
		}
		return struct{}{}, err
	case "Node.Stats":
		var r JobReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		return &StatsResp{Stats: p.Stats()}, nil
	case "Node.Dead":
		var r DeadReq
		if err := dec(&r); err != nil {
			return nil, err
		}
		p, err := n.leader(r.Key)
		if err != nil {
			return nil, err
		}
		return &DeadResp{Dead: p.DeadLetters(r.Limit)}, nil
	}
	return nil, fmt.Errorf("node %s: unknown method %s", n.cfg.Name, method)
}

var _ = job.Job{}

// Kill simulates a process crash for fault-injection tests: logs are closed
// without flushing, nothing is handed over, and the node stops answering.
func (n *Node) Kill() {
	n.mu.Lock()
	n.closed = true
	hs := make([]*hostedPartition, 0, len(n.hosted))
	for _, h := range n.hosted {
		hs = append(hs, h)
	}
	n.mu.Unlock()
	for _, h := range hs {
		h.replica.Log().Abort()
		h.replica.StopLeading()
		n.dropPartition(h)
	}
	n.wg.Wait()
}

// LastHeartbeat returns when the coordinator last answered this node (wall
// clock; zero if never). Readiness probes use it.
func (n *Node) LastHeartbeat() time.Time {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastOK
}
