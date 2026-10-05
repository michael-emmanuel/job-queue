package replication

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

// Transport delivers RPCs between replicas. Implementations must be safe for
// concurrent use. cluster provides an in-memory fault-injecting network for
// tests and an HTTP transport for real deployments.
type Transport interface {
	Call(ctx context.Context, to, method string, req, resp any) error
}

// AckLevel is how many replicas must hold a write before the leader
// acknowledges it. It trades latency and availability against durability.
type AckLevel int

const (
	// AckQuorum acknowledges after a majority (leader included) has fsynced.
	// A committed write survives any minority of replica failures.
	AckQuorum AckLevel = iota
	// AckLeader acknowledges after only the leader has fsynced. Fastest, but
	// an acknowledged write is lost if the leader dies before replicating it.
	AckLeader
	// AckAll waits for every replica. Strongest, but one slow or dead replica
	// stalls writes, so availability is worse than with a single node.
	AckAll
)

// Errors.
var (
	ErrNotLeader = errors.New("replication: not the leader")
	// ErrLeadershipLost means leadership ended while a write was pending. The
	// write MAY still become committed (a new leader can inherit it), so
	// callers must treat the outcome as unknown, exactly like a timeout.
	ErrLeadershipLost = errors.New("replication: leadership lost; outcome of the write is unknown")
	// ErrCompactionUnsupported is returned by the partition-log adapter:
	// snapshots of speculative state are unsound, see docs/design-notes-m3.md.
	ErrCompactionUnsupported = errors.New("replication: log compaction is not supported for replicated partitions")
)

// Role of a replica.
type Role int

// Roles.
const (
	Follower Role = iota
	Leader
)

func (r Role) String() string {
	if r == Leader {
		return "leader"
	}
	return "follower"
}

// RPC messages.
type (
	// AppendReq is the leader's replication and heartbeat message.
	AppendReq struct {
		Key       string  `json:"key"`
		Epoch     uint64  `json:"epoch"`
		Leader    string  `json:"leader"`
		PrevIndex uint64  `json:"prev_index"`
		PrevEpoch uint64  `json:"prev_epoch"`
		Entries   []Entry `json:"entries,omitempty"`
		Commit    uint64  `json:"commit"`
	}
	// AppendResp answers AppendReq.
	AppendResp struct {
		Epoch   uint64 `json:"epoch"`
		Success bool   `json:"success"`
		Match   uint64 `json:"match"`
		Hint    uint64 `json:"hint"` // where the leader should retry from after a mismatch
	}
	// PrepareReq asks a replica to promise an epoch.
	PrepareReq struct {
		Key   string `json:"key"`
		Epoch uint64 `json:"epoch"`
	}
	// PrepareResp reports the promise and the replica's log position.
	PrepareResp struct {
		OK        bool   `json:"ok"`
		Epoch     uint64 `json:"epoch"`
		LastEpoch uint64 `json:"last_epoch"`
		LastIndex uint64 `json:"last_index"`
	}
)

// Config configures a Replica.
type Config struct {
	ID        string // this node's name
	Key       string // partition identity, e.g. "email/2"
	Path      string // log file
	Clock     clock.Clock
	Transport Transport
	Ack       AckLevel

	BatchEntries int           // max entries per AppendEntries (default 256)
	BatchBytes   int           // soft byte cap per AppendEntries (default 1 MiB)
	Heartbeat    time.Duration // idle AppendEntries interval, real time (default 100ms)
	RPCTimeout   time.Duration // default 1s

	// OnStepDown is called in its own goroutine when this replica stops being
	// leader because it learned of a higher epoch. The owner must discard any
	// in-memory state built on top of the log.
	OnStepDown func(epoch uint64)
	Logger     *slog.Logger
}

// Replica is one copy of a partition's log, either following or leading.
type Replica struct {
	cfg Config
	log *Log

	mu         sync.Mutex
	role       Role
	epoch      uint64 // highest epoch promised or seen; equals log.Promised()
	leaderID   string
	commit     uint64
	replicas   []string // full replica set while leading
	match      map[string]uint64
	leaseUntil time.Time
	notify     chan struct{} // broadcast: commit, match, role or durable change
	kickCh     chan struct{} // broadcast: new entries for replicators
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// OpenReplica opens the replica's log; it starts as a follower.
func OpenReplica(cfg Config) (*Replica, error) {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.BatchEntries <= 0 {
		cfg.BatchEntries = 256
	}
	if cfg.BatchBytes <= 0 {
		cfg.BatchBytes = 1 << 20
	}
	if cfg.Heartbeat <= 0 {
		cfg.Heartbeat = 100 * time.Millisecond
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = observability.Discard()
	}
	cfg.Logger = cfg.Logger.With("replica", cfg.Key, "node", cfg.ID)
	l, err := OpenLog(cfg.Path)
	if err != nil {
		return nil, err
	}
	r := &Replica{cfg: cfg, log: l, epoch: l.Promised(),
		notify: make(chan struct{}), kickCh: make(chan struct{})}
	l.SetOnDurable(r.onLocalDurable)
	return r, nil
}

func (r *Replica) broadcastLocked() { close(r.notify); r.notify = make(chan struct{}) }
func (r *Replica) kickLocked()      { close(r.kickCh); r.kickCh = make(chan struct{}) }

// onLocalDurable runs when the leader's own fsync completes: the leader's
// vote in the quorum only counts once its copy is durable.
func (r *Replica) onLocalDurable() {
	r.mu.Lock()
	r.advanceCommitLocked()
	r.broadcastLocked()
	r.mu.Unlock()
}

// advanceCommitLocked recomputes the commit index: the largest N held
// durably by a majority. A leader only counts replicas for entries of its own
// epoch; earlier entries commit indirectly once a later one does (Raft's
// rule, which prevents a committed entry from being overwritten after a
// leader change). The no-op appended at election makes that happen promptly.
func (r *Replica) advanceCommitLocked() {
	if r.role != Leader {
		return
	}
	vals := []uint64{r.log.DurableIndex()}
	for _, p := range r.replicas {
		if p != r.cfg.ID {
			vals = append(vals, r.match[p])
		}
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] > vals[j] })
	n := vals[len(r.replicas)/2] // majority position: len/2+1 replicas are >= this value
	if n > r.commit {
		if e, ok := r.log.EpochAt(n); ok && e == r.epoch {
			r.commit = n
		}
	}
}

// ---- election side ----

// HandlePrepare durably promises epoch and reports the log position. After a
// successful Prepare this replica rejects every message from a lower epoch,
// which is what fences a deposed leader. If it was leading, it steps down.
func (r *Replica) HandlePrepare(req *PrepareReq) (*PrepareResp, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if req.Epoch < r.epoch {
		return &PrepareResp{OK: false, Epoch: r.epoch}, nil
	}
	if req.Epoch > r.epoch { // equal is an idempotent retry
		if err := r.log.Promise(req.Epoch); err != nil {
			return nil, err
		}
		r.epoch = req.Epoch
	}
	// Promising any epoch (even a retry of the current one) ends our own
	// leadership: someone else is about to lead, and a leader must never
	// keep writing after it has fenced itself.
	r.stepDownLocked(req.Epoch)
	return &PrepareResp{OK: true, Epoch: r.epoch, LastEpoch: r.log.LastEpoch(), LastIndex: r.log.LastIndex()}, nil
}

// StartLeading makes this replica the leader of epoch. The epoch must have
// been promised through HandlePrepare. replicas lists every replica
// including this one; leaseUntil bounds how long the replica may keep
// accepting writes without hearing from the coordinator again.
func (r *Replica) StartLeading(epoch uint64, replicas []string, leaseUntil time.Time) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.epoch != epoch {
		return fmt.Errorf("replication: cannot lead epoch %d: promised %d", epoch, r.epoch)
	}
	if r.role == Leader {
		return errors.New("replication: already leading")
	}
	r.role = Leader
	r.leaderID = r.cfg.ID
	r.replicas = append([]string(nil), replicas...)
	r.match = map[string]uint64{}
	r.leaseUntil = leaseUntil
	if _, err := r.log.Append(epoch, TypeNoop, nil); err != nil {
		r.role = Follower
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	for _, p := range r.replicas {
		if p != r.cfg.ID {
			r.wg.Add(1)
			go r.replicate(ctx, p, epoch)
		}
	}
	r.advanceCommitLocked()
	r.broadcastLocked()
	return nil
}

// stepDownLocked ends leadership. Pending writers are released with
// ErrLeadershipLost. Callers must not wait for replicators while holding mu.
func (r *Replica) stepDownLocked(newEpoch uint64) {
	if r.role != Leader {
		return
	}
	r.role = Follower
	r.leaseUntil = time.Time{}
	if r.cancel != nil {
		r.cancel()
		r.cancel = nil
	}
	r.kickLocked()
	r.broadcastLocked()
	if cb := r.cfg.OnStepDown; cb != nil {
		go cb(newEpoch)
	}
}

// StopLeading demotes voluntarily and waits for replicators to exit.
func (r *Replica) StopLeading() {
	r.mu.Lock()
	r.stepDownLocked(r.epoch)
	r.mu.Unlock()
	r.wg.Wait()
}

func (r *Replica) stepDownTo(epoch uint64) {
	r.mu.Lock()
	if epoch > r.epoch {
		// Raise our own promise too, so we never act in the old epoch again.
		if err := r.log.Promise(epoch); err == nil {
			r.epoch = epoch
		}
	}
	r.stepDownLocked(epoch)
	r.mu.Unlock()
}

// SetLease extends the leader's lease.
func (r *Replica) SetLease(until time.Time) {
	r.mu.Lock()
	if r.role == Leader && until.After(r.leaseUntil) {
		r.leaseUntil = until
	}
	r.mu.Unlock()
}

func (r *Replica) leaseOKLocked() bool { return r.cfg.Clock.Now().Before(r.leaseUntil) }

// ---- leader write path ----

func (r *Replica) leaderAppend(epoch uint64, rec storage.Record) (storage.WaitFunc, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader || r.epoch != epoch || !r.leaseOKLocked() {
		return nil, ErrNotLeader
	}
	idx, err := r.log.Append(epoch, rec.Type, rec.Data)
	if err != nil {
		return nil, err
	}
	r.kickLocked()
	return func(ctx context.Context) error { return r.waitAck(ctx, idx, epoch) }, nil
}

func (r *Replica) waitAck(ctx context.Context, idx, epoch uint64) error {
	for {
		r.mu.Lock()
		if r.role != Leader || r.epoch != epoch {
			r.mu.Unlock()
			return ErrLeadershipLost
		}
		if err := r.log.Err(); err != nil {
			r.mu.Unlock()
			return err
		}
		var ok bool
		switch r.cfg.Ack {
		case AckLeader:
			ok = r.log.DurableIndex() >= idx
		case AckAll:
			ok = r.log.DurableIndex() >= idx
			for _, p := range r.replicas {
				if p != r.cfg.ID && r.match[p] < idx {
					ok = false
				}
			}
		default:
			ok = r.commit >= idx
		}
		ch := r.notify
		r.mu.Unlock()
		if ok {
			return nil
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// replicate drives one follower for the lifetime of a leadership term: send
// entries, back up on mismatch, advance the match index, heartbeat when idle.
func (r *Replica) replicate(ctx context.Context, peer string, epoch uint64) {
	defer r.wg.Done()
	next := r.log.LastIndex() + 1 // optimistic: assume the follower is current; mismatches walk it back
	backoff := 10 * time.Millisecond
	for ctx.Err() == nil {
		r.mu.Lock()
		if r.role != Leader || r.epoch != epoch {
			r.mu.Unlock()
			return
		}
		commit, kick := r.commit, r.kickCh
		r.mu.Unlock()

		last := r.log.LastIndex()
		next = min(max(next, 1), last+1)
		prev := next - 1
		prevEpoch, _ := r.log.EpochAt(prev)
		var entries []Entry
		if next <= last {
			var err error
			if entries, err = r.log.Read(next, r.cfg.BatchEntries, r.cfg.BatchBytes); err != nil {
				r.cfg.Logger.Error("reading log for follower", "peer", peer, "err", err)
				if !sleepCtx(ctx, backoff) {
					return
				}
				continue
			}
		}
		req := AppendReq{Key: r.cfg.Key, Epoch: epoch, Leader: r.cfg.ID, PrevIndex: prev,
			PrevEpoch: prevEpoch, Entries: entries, Commit: commit}
		cctx, cancel := context.WithTimeout(ctx, r.cfg.RPCTimeout)
		var resp AppendResp
		err := r.cfg.Transport.Call(cctx, peer, "Replica.AppendEntries", &req, &resp)
		cancel()
		if err != nil {
			// Unreachable or slow follower: back off, never give up. It
			// resumes from wherever its log stands when it returns.
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = min(backoff*2, 500*time.Millisecond)
			continue
		}
		backoff = 10 * time.Millisecond
		if resp.Epoch > epoch {
			r.stepDownTo(resp.Epoch)
			return
		}
		if resp.Success {
			next = resp.Match + 1
			r.mu.Lock()
			if r.role == Leader && r.epoch == epoch && resp.Match > r.match[peer] {
				r.match[peer] = resp.Match
				r.advanceCommitLocked()
				r.broadcastLocked()
			}
			r.mu.Unlock()
		} else {
			next = max(1, min(resp.Hint, next-1))
			continue
		}
		if next > r.log.LastIndex() { // caught up: wait for new entries or the heartbeat
			select {
			case <-kick:
			case <-time.After(r.cfg.Heartbeat):
			case <-ctx.Done():
				return
			}
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// ---- follower side ----

// HandleAppend processes a leader's AppendEntries.
func (r *Replica) HandleAppend(ctx context.Context, req *AppendReq) (*AppendResp, error) {
	r.mu.Lock()
	if req.Epoch < r.epoch {
		resp := &AppendResp{Epoch: r.epoch}
		r.mu.Unlock()
		return resp, nil // stale leader: it learns the new epoch from the reply and steps down
	}
	if req.Epoch > r.epoch {
		if err := r.log.Promise(req.Epoch); err != nil {
			r.mu.Unlock()
			return nil, err
		}
		r.epoch = req.Epoch
	}
	if r.role == Leader {
		r.stepDownLocked(req.Epoch) // a leader of a lower or equal epoch that sees another leader is deposed
	}
	r.leaderID = req.Leader

	last := r.log.LastIndex()
	if req.PrevIndex > last {
		r.mu.Unlock()
		return &AppendResp{Epoch: req.Epoch, Hint: last + 1}, nil
	}
	if pe, _ := r.log.EpochAt(req.PrevIndex); pe != req.PrevEpoch {
		hint := r.firstIndexOfEpochAt(req.PrevIndex)
		r.mu.Unlock()
		return &AppendResp{Epoch: req.Epoch, Hint: hint}, nil
	}
	newLast, err := r.log.Merge(req.PrevIndex, req.Entries)
	if err != nil {
		r.mu.Unlock()
		return nil, err
	}
	r.commit = max(r.commit, min(req.Commit, newLast))
	r.mu.Unlock()

	// Acknowledge only what is durable here: the leader counts this reply
	// toward the quorum, so replying early would let a crash lose a
	// "committed" entry.
	if len(req.Entries) > 0 {
		if err := r.log.WaitDurable(ctx, newLast); err != nil {
			return nil, err
		}
	}
	return &AppendResp{Epoch: req.Epoch, Success: true, Match: newLast}, nil
}

func (r *Replica) firstIndexOfEpochAt(i uint64) uint64 {
	r.log.mu.Lock()
	defer r.log.mu.Unlock()
	if i == 0 || i > uint64(len(r.log.epochs)) {
		return 1
	}
	return r.log.firstIndexOfEpochLocked(i, r.log.epochs[i-1])
}

// ---- inspection ----

// Info is a point-in-time view of a replica.
type Info struct {
	Role       Role
	Epoch      uint64
	LeaderID   string
	LastIndex  uint64
	LastEpoch  uint64
	Durable    uint64
	Commit     uint64
	LeaseUntil time.Time
	// Lag is, per follower, how many entries it is behind the leader's log.
	Lag map[string]uint64
}

// Info returns a snapshot.
func (r *Replica) Info() Info {
	r.mu.Lock()
	defer r.mu.Unlock()
	in := Info{Role: r.role, Epoch: r.epoch, LeaderID: r.leaderID, LastIndex: r.log.LastIndex(),
		LastEpoch: r.log.LastEpoch(), Durable: r.log.DurableIndex(), Commit: r.commit, LeaseUntil: r.leaseUntil}
	if r.role == Leader {
		in.Lag = map[string]uint64{}
		for _, p := range r.replicas {
			if p != r.cfg.ID {
				in.Lag[p] = in.LastIndex - min(r.match[p], in.LastIndex)
			}
		}
	}
	return in
}

// Log exposes the underlying log (for tests and the node's recovery path).
func (r *Replica) Log() *Log { return r.log }

// Close stops leading and closes the log.
func (r *Replica) Close() error {
	r.StopLeading()
	return r.log.Close()
}

// ---- storage.Log adapter for a Partition ----

// LeaderLog returns a storage.Log through which a Partition reads and writes
// this replica's log while it leads epoch. Writes are acknowledged according
// to the replica's AckLevel; once leadership or the lease is lost every
// operation fails with ErrNotLeader.
func (r *Replica) LeaderLog() (storage.Log, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.role != Leader {
		return nil, ErrNotLeader
	}
	return &leaderLog{r: r, epoch: r.epoch}, nil
}

type leaderLog struct {
	r     *Replica
	epoch uint64
}

var _ storage.Log = (*leaderLog)(nil)

func (l *leaderLog) Append(rec storage.Record) (storage.WaitFunc, error) {
	return l.r.leaderAppend(l.epoch, rec)
}

func (l *leaderLog) Replay(fn func(storage.Record) error) error {
	return l.r.log.Replay(func(e Entry) error {
		if e.Type == TypeNoop {
			return nil
		}
		return fn(storage.Record{Type: e.Type, Data: e.Data})
	})
}

func (l *leaderLog) Rotate() (uint64, error) { return 0, ErrCompactionUnsupported }
func (l *leaderLog) WriteSnapshot(uint64, []storage.Record) error {
	return ErrCompactionUnsupported
}
func (l *leaderLog) Size() int64  { return l.r.log.Size() }
func (l *leaderLog) Close() error { return nil } // the Replica owns the file

// Err reports why the partition must stop serving: leadership or lease lost,
// or a failed disk. Partition.Healthy surfaces it to readiness checks.
func (l *leaderLog) Err() error {
	l.r.mu.Lock()
	defer l.r.mu.Unlock()
	if l.r.role != Leader || l.r.epoch != l.epoch || !l.r.leaseOKLocked() {
		return ErrNotLeader
	}
	return l.r.log.Err()
}
