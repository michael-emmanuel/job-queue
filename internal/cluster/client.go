package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
	"github.com/michael-emmanuel/jobqueue/internal/routing"
)

// ClientConfig configures a Client.
type ClientConfig struct {
	Name      string
	Transport replication.Transport
	Coord     string
	// Retries is how many times a routed call is retried after a failure that
	// proves the request was not applied (default 8). RetryDelay is the real
	// pause between attempts (default 20ms).
	Retries    int
	RetryDelay time.Duration
	RPCTimeout time.Duration
	// AmbiguousRetries bounds retries after an error that leaves the outcome
	// unknown (a timeout, lost leadership) for idempotent operations
	// (default 2). Retries after "definitely not applied" errors are bounded
	// only by Retries. Without this bound a quorum outage would hold every
	// request for the full request deadline.
	AmbiguousRetries int
	// GroupSession is the session timeout for consumer-group members that
	// the client manages on behalf of stateless callers (default 10s).
	GroupSession time.Duration
}

// Client routes requests to partition leaders. It is the logic a stateless
// gateway runs: it needs no local state beyond a cache of the cluster's
// metadata, which it refreshes whenever a call shows the cache is stale.
type Client struct {
	cfg  ClientConfig
	mu   sync.Mutex
	meta *Metadata
	rr   atomic.Uint32

	cmu       sync.Mutex
	consumers map[string]*Consumer
}

// NewClient returns a Client.
func NewClient(cfg ClientConfig) *Client {
	if cfg.Retries <= 0 {
		cfg.Retries = 8
	}
	if cfg.RetryDelay <= 0 {
		cfg.RetryDelay = 20 * time.Millisecond
	}
	if cfg.RPCTimeout <= 0 {
		cfg.RPCTimeout = 2 * time.Second
	}
	if cfg.AmbiguousRetries <= 0 {
		cfg.AmbiguousRetries = 2
	}
	if cfg.GroupSession <= 0 {
		cfg.GroupSession = 10 * time.Second
	}
	return &Client{cfg: cfg, consumers: map[string]*Consumer{}}
}

func (c *Client) call(ctx context.Context, to, method string, req, resp any) error {
	cctx, cancel := context.WithTimeout(ctx, c.cfg.RPCTimeout)
	defer cancel()
	err := c.cfg.Transport.Call(cctx, to, method, req, resp)
	if err != nil && errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
		return fmt.Errorf("%w: %s on %s timed out", context.DeadlineExceeded, method, to)
	}
	return err
}

// Refresh reloads cluster metadata from the coordinator.
func (c *Client) Refresh(ctx context.Context) error {
	var m Metadata
	if err := c.call(ctx, c.cfg.Coord, "Coord.Metadata", struct{}{}, &m); err != nil {
		return fmt.Errorf("%w: %v", ErrNoCoordinator, err)
	}
	c.mu.Lock()
	c.meta = &m
	c.mu.Unlock()
	return nil
}

// CreateQueue creates a queue through the coordinator.
func (c *Client) CreateQueue(ctx context.Context, name string, cfg broker.QueueConfig) (broker.QueueConfig, error) {
	var q QueueMeta
	err := c.call(ctx, c.cfg.Coord, "Coord.CreateQueue", &CreateQueueReq{Name: name, Config: cfg}, &q)
	if err == nil {
		err = c.Refresh(ctx)
	}
	return q.Config, err
}

// DeleteQueue deletes a queue.
func (c *Client) DeleteQueue(ctx context.Context, name string) error {
	err := c.call(ctx, c.cfg.Coord, "Coord.DeleteQueue", name, nil)
	if err == nil {
		err = c.Refresh(ctx)
	}
	return err
}

func (c *Client) leaderOf(queue string, part int) (leader string, parts int, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.meta == nil {
		return "", 0, ErrNoCoordinator
	}
	q := c.meta.Queues[queue]
	if q == nil {
		return "", 0, broker.ErrQueueNotFound
	}
	if part < 0 || part >= len(q.Partitions) {
		return "", 0, broker.ErrInvalidID
	}
	return q.Partitions[part].Leader, len(q.Partitions), nil
}

// retryable reports whether err proves the request was NOT applied, so
// resending cannot duplicate it: the target was not the leader, did not host
// the partition, or could not be reached.
func retryable(err error) bool {
	return errors.Is(err, ErrNotLeader) || errors.Is(err, ErrNotHosted) || errors.Is(err, ErrUnreachable) ||
		errors.Is(err, ErrNoCoordinator)
}

// ambiguous reports errors after which the request may or may not have been
// applied. They are only safe to retry when the operation is idempotent.
func ambiguous(err error) bool {
	return errors.Is(err, replication.ErrLeadershipLost) || errors.Is(err, context.DeadlineExceeded)
}

// onLeader sends a request to a partition's current leader, refreshing
// metadata and retrying when the cached leader turns out to be wrong.
func (c *Client) onLeader(ctx context.Context, queue string, part int, method string, req, resp any, idempotent bool) error {
	var last error
	unsure := 0
	for attempt := 0; attempt <= c.cfg.Retries; attempt++ {
		if attempt > 0 {
			select {
			case <-time.After(c.cfg.RetryDelay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		c.mu.Lock()
		have := c.meta != nil
		c.mu.Unlock()
		if !have || attempt > 0 {
			if err := c.Refresh(ctx); err != nil {
				last = err
				continue
			}
		}
		leader, _, err := c.leaderOf(queue, part)
		if err != nil {
			return err
		}
		if leader == "" {
			last = fmt.Errorf("%w: no leader elected yet for %s", ErrNotLeader, Key(queue, part))
			continue
		}
		err = c.call(ctx, leader, method, req, resp)
		if err == nil {
			return nil
		}
		if retryable(err) {
			last = err
			continue
		}
		if idempotent && ambiguous(err) && unsure < c.cfg.AmbiguousRetries {
			unsure++
			last = err
			continue
		}
		return err
	}
	return last
}

// Enqueue routes a job by partition key, then idempotency key, then
// round-robin (the same rule as the single-node broker).
func (c *Client) Enqueue(ctx context.Context, queue string, j job.Job) (broker.EnqueueResult, error) {
	if c.metaNil() {
		if err := c.Refresh(ctx); err != nil {
			return broker.EnqueueResult{}, err
		}
	}
	_, n, err := c.leaderOf(queue, 0)
	if err != nil {
		return broker.EnqueueResult{}, err
	}
	var part int
	switch {
	case j.PartitionKey != "":
		part = routing.Partition(queue, j.PartitionKey, n)
	case j.IdempotencyKey != "":
		part = routing.Partition(queue, j.IdempotencyKey, n)
	default:
		part = int(c.rr.Add(1)) % n
	}
	j.Queue = queue
	j.ID = fmt.Sprintf("%s.%d.%s", queue, part, job.NewID())
	var resp EnqueueResp
	// With an idempotency key a retry after an ambiguous failure is safe: the
	// partition returns the original job. Without one it is not, so the
	// caller sees the ambiguity instead.
	err = c.onLeader(ctx, queue, part, "Node.Enqueue", &EnqueueReq{Key: Key(queue, part), Job: j}, &resp, j.IdempotencyKey != "")
	if err != nil {
		return broker.EnqueueResult{ID: j.ID, Partition: part}, err
	}
	return broker.EnqueueResult{ID: resp.ID, Partition: part, Duplicate: resp.Duplicate}, nil
}

func (c *Client) metaNil() bool { c.mu.Lock(); defer c.mu.Unlock(); return c.meta == nil }

func routeOf(id string) (queue string, part int, err error) { return broker.ParseID(id) }

// Ack completes a job.
func (c *Client) Ack(ctx context.Context, id string, attempt int) error {
	q, p, err := routeOf(id)
	if err != nil {
		return err
	}
	return c.onLeader(ctx, q, p, "Node.Ack", &AckReq{Key: Key(q, p), ID: id, Attempt: attempt}, nil, false)
}

// Fail reports a failure and returns "retry" or "dead".
func (c *Client) Fail(ctx context.Context, id string, attempt int, msg string, nonRetryable bool) (string, error) {
	q, p, err := routeOf(id)
	if err != nil {
		return "", err
	}
	var r FailResp
	err = c.onLeader(ctx, q, p, "Node.Fail", &FailReq{Key: Key(q, p), ID: id, Attempt: attempt, Err: msg, NonRetryable: nonRetryable}, &r, false)
	return r.Outcome, err
}

// Renew extends a lease.
func (c *Client) Renew(ctx context.Context, id string, attempt int, extend time.Duration) (time.Time, error) {
	q, p, err := routeOf(id)
	if err != nil {
		return time.Time{}, err
	}
	var r RenewResp
	err = c.onLeader(ctx, q, p, "Node.Renew", &RenewReq{Key: Key(q, p), ID: id, Attempt: attempt, Extend: extend}, &r, true)
	return r.Deadline, err
}

// Get returns a job's current state from its partition leader.
func (c *Client) Get(ctx context.Context, id string) (job.Job, error) {
	q, p, err := routeOf(id)
	if err != nil {
		return job.Job{}, err
	}
	var r JobResp
	err = c.onLeader(ctx, q, p, "Node.Get", &JobReq{Key: Key(q, p), ID: id}, &r, true)
	return r.Job, err
}

// Cancel removes a live job.
func (c *Client) Cancel(ctx context.Context, id string) error {
	q, p, err := routeOf(id)
	if err != nil {
		return err
	}
	return c.onLeader(ctx, q, p, "Node.Cancel", &JobReq{Key: Key(q, p), ID: id}, nil, false)
}

// ReplayDead moves a dead-lettered job back to ready.
func (c *Client) ReplayDead(ctx context.Context, id string) error {
	q, p, err := routeOf(id)
	if err != nil {
		return err
	}
	return c.onLeader(ctx, q, p, "Node.ReplayDead", &JobReq{Key: Key(q, p), ID: id}, nil, false)
}

// DeadLetters lists a queue's DLQ across all partitions, oldest first.
func (c *Client) DeadLetters(ctx context.Context, queue string, limit int) ([]job.DeadLetter, error) {
	if c.metaNil() {
		if err := c.Refresh(ctx); err != nil {
			return nil, err
		}
	}
	_, n, err := c.leaderOf(queue, 0)
	if err != nil {
		return nil, err
	}
	var all []job.DeadLetter
	for p := 0; p < n; p++ {
		var r DeadResp
		if err := c.onLeader(ctx, queue, p, "Node.Dead", &DeadReq{Key: Key(queue, p), Limit: limit}, &r, true); err != nil {
			return nil, err
		}
		all = append(all, r.Dead...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].DeadAt.Before(all[j].DeadAt) })
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
}

// ---- consumer groups ----

// Consumer is one member of a consumer group. It owns the partitions the
// coordinator assigned to it and pulls only from those.
type Consumer struct {
	c       *Client
	queue   string
	group   string
	member  string
	session time.Duration

	mu       sync.Mutex
	gen      uint64
	assigned []int
	rr       int
	joined   bool
	lastBeat time.Time
	used     time.Time
}

func (w *Consumer) touch() { w.mu.Lock(); w.used = time.Now(); w.mu.Unlock() }
func (w *Consumer) lastUsed() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.used
}

// ensure joins the group if needed and heartbeats when a third of the session
// has passed, so membership follows activity without a background goroutine.
func (w *Consumer) ensure(ctx context.Context) error {
	w.mu.Lock()
	joined, due := w.joined, time.Since(w.lastBeat) > w.session/3
	w.used = time.Now()
	w.mu.Unlock()
	switch {
	case !joined:
		return w.Join(ctx)
	case due:
		_, err := w.Heartbeat(ctx)
		return err
	}
	return nil
}

// NewConsumer creates a group member. Call Join before Poll.
func (c *Client) NewConsumer(queue, group, member string, session time.Duration) *Consumer {
	return &Consumer{c: c, queue: queue, group: group, member: member, session: session}
}

// Join registers with the group and learns the assignment.
func (w *Consumer) Join(ctx context.Context) error {
	var r GroupResp
	err := w.c.call(ctx, w.c.cfg.Coord, "Coord.JoinGroup",
		&JoinGroupReq{Queue: w.queue, Group: w.group, Member: w.member, SessionTimeout: w.session}, &r)
	if err != nil {
		return err
	}
	w.set(r)
	return nil
}

func (w *Consumer) set(r GroupResp) {
	w.mu.Lock()
	w.gen, w.assigned = r.Generation, r.Assigned
	w.joined, w.lastBeat = true, time.Now()
	w.mu.Unlock()
}

// Assignment returns the member's current generation and partitions.
func (w *Consumer) Assignment() (gen uint64, partitions []int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.gen, append([]int(nil), w.assigned...)
}

// Heartbeat keeps the membership alive and adopts any new assignment. If the
// coordinator no longer knows this member (session expired, coordinator
// restarted) it rejoins. changed reports a rebalance.
func (w *Consumer) Heartbeat(ctx context.Context) (changed bool, err error) {
	var r GroupResp
	err = w.c.call(ctx, w.c.cfg.Coord, "Coord.GroupHeartbeat",
		&GroupHeartbeatReq{Queue: w.queue, Group: w.group, Member: w.member}, &r)
	if errors.Is(err, ErrUnknownMember) {
		old, _ := w.Assignment()
		if err = w.Join(ctx); err != nil {
			return false, err
		}
		g, _ := w.Assignment()
		return g != old, nil
	}
	if err != nil {
		return false, err
	}
	w.mu.Lock()
	changed = r.Generation != w.gen
	w.gen, w.assigned = r.Generation, r.Assigned
	w.lastBeat = time.Now()
	w.mu.Unlock()
	return changed, nil
}

// Leave removes the member immediately so its partitions move without
// waiting for the session timeout.
func (w *Consumer) Leave(ctx context.Context) error {
	return w.c.call(ctx, w.c.cfg.Coord, "Coord.LeaveGroup",
		&GroupHeartbeatReq{Queue: w.queue, Group: w.group, Member: w.member}, nil)
}

// Poll leases up to max jobs from the member's assigned partitions only. With
// wait > 0 it keeps polling until jobs arrive or wait elapses. (Server-side
// long polling exists per partition; across several partitions led by
// different nodes the client polls, trading a little latency for not holding
// a connection open to every leader.)
func (w *Consumer) Poll(ctx context.Context, max int, visibility, wait time.Duration) ([]job.Job, error) {
	deadline := time.Now().Add(wait)
	for {
		jobs, err := w.pollOnce(ctx, max, visibility)
		if err != nil || len(jobs) > 0 || wait <= 0 || !time.Now().Before(deadline) {
			return jobs, err
		}
		select {
		case <-time.After(10 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (w *Consumer) pollOnce(ctx context.Context, max int, visibility time.Duration) ([]job.Job, error) {
	gen, parts := w.Assignment()
	var out []job.Job
	w.mu.Lock()
	start := w.rr
	w.rr++
	w.mu.Unlock()
	var firstErr error
	for i := 0; i < len(parts) && len(out) < max; i++ {
		p := parts[(start+i)%len(parts)]
		var r PullResp
		err := w.c.onLeader(ctx, w.queue, p, "Node.Pull", &PullReq{Key: Key(w.queue, p), Group: w.group,
			Member: w.member, Generation: gen, Worker: w.member, Max: max - len(out), Visibility: visibility}, &r, false)
		if errors.Is(err, ErrStaleGeneration) {
			// A rebalance happened that we have not seen yet: refresh and
			// let the caller poll again under the new assignment.
			if _, herr := w.Heartbeat(ctx); herr != nil {
				return out, herr
			}
			return out, nil
		}
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue // one unavailable partition must not starve the others
		}
		out = append(out, r.Jobs...)
	}
	if len(out) == 0 {
		return nil, firstErr
	}
	return out, nil
}

var _ = partition.ErrNotFound

// DeleteDead permanently removes a dead-lettered job.
func (c *Client) DeleteDead(ctx context.Context, id string) error {
	q, p, err := routeOf(id)
	if err != nil {
		return err
	}
	return c.onLeader(ctx, q, p, "Node.DeleteDead", &JobReq{Key: Key(q, p), ID: id}, nil, false)
}

// Queues lists queues from cached (or freshly loaded) metadata.
func (c *Client) Queues(ctx context.Context) ([]broker.QueueInfo, error) {
	if err := c.Refresh(ctx); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]broker.QueueInfo, 0, len(c.meta.Queues))
	for n, q := range c.meta.Queues {
		out = append(out, broker.QueueInfo{Name: n, Config: q.Config})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// QueueConfig returns a queue's configuration from the metadata cache,
// loading metadata if it has not been fetched yet.
func (c *Client) QueueConfig(ctx context.Context, queue string) (broker.QueueConfig, error) {
	if c.metaNil() {
		if err := c.Refresh(ctx); err != nil {
			return broker.QueueConfig{}, err
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	q := c.meta.Queues[queue]
	if q == nil {
		return broker.QueueConfig{}, broker.ErrQueueNotFound
	}
	return q.Config, nil
}

// QueueStats gathers statistics from every partition's leader.
func (c *Client) QueueStats(ctx context.Context, queue string) (broker.QueueStats, error) {
	if err := c.Refresh(ctx); err != nil {
		return broker.QueueStats{}, err
	}
	cfg, err := c.QueueConfig(ctx, queue)
	if err != nil {
		return broker.QueueStats{}, err
	}
	st := broker.QueueStats{Name: queue, Config: cfg}
	for p := 0; p < cfg.Partitions; p++ {
		var r StatsResp
		if err := c.onLeader(ctx, queue, p, "Node.Stats", &JobReq{Key: Key(queue, p)}, &r, true); err != nil {
			return st, err
		}
		st.Partitions = append(st.Partitions, r.Stats)
	}
	st.Total = broker.SumStats(st.Partitions)
	return st, nil
}

// PullOpts describes a pull by a stateless caller such as a gateway.
type PullOpts struct {
	Group      string // empty = poll every partition
	Worker     string
	Max        int
	Visibility time.Duration
	Wait       time.Duration
}

// Pull leases jobs for a worker. With a group it pulls only from the
// partitions the group assigned to Worker (joining or heartbeating as
// needed, so a stateless gateway can call it on every request); without one
// it scans all partitions. Waiting is client-side polling with backoff up to
// 100ms, so a job that arrives mid-wait is seen within about 100ms.
func (c *Client) Pull(ctx context.Context, queue string, o PullOpts) ([]job.Job, error) {
	if o.Max <= 0 {
		o.Max = 1
	}
	var once func() ([]job.Job, error)
	if o.Group != "" {
		w := c.groupConsumer(queue, o.Group, o.Worker)
		if err := w.ensure(ctx); err != nil {
			return nil, err
		}
		once = func() ([]job.Job, error) { return w.pollOnce(ctx, o.Max, o.Visibility) }
		defer w.touch()
	} else {
		once = func() ([]job.Job, error) { return c.scanOnce(ctx, queue, o) }
	}
	deadline := time.Now().Add(o.Wait)
	sleep := 5 * time.Millisecond
	for {
		jobs, err := once()
		if err != nil || len(jobs) > 0 || o.Wait <= 0 || !time.Now().Before(deadline) {
			return jobs, err
		}
		select {
		case <-time.After(sleep):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		sleep = min(sleep*2, 100*time.Millisecond)
		if o.Group != "" {
			if err := c.groupConsumer(queue, o.Group, o.Worker).ensure(ctx); err != nil {
				return nil, err
			}
		}
	}
}

func (c *Client) scanOnce(ctx context.Context, queue string, o PullOpts) ([]job.Job, error) {
	cfg, err := c.QueueConfig(ctx, queue)
	if err != nil {
		return nil, err
	}
	start := int(c.rr.Add(1))
	var (
		out      []job.Job
		firstErr error
	)
	for i := 0; i < cfg.Partitions && len(out) < o.Max; i++ {
		p := (start + i) % cfg.Partitions
		var r PullResp
		err := c.onLeader(ctx, queue, p, "Node.Pull", &PullReq{Key: Key(queue, p), Worker: o.Worker,
			Max: o.Max - len(out), Visibility: o.Visibility}, &r, false)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue // one unavailable partition must not hide work on the others
		}
		out = append(out, r.Jobs...)
	}
	if len(out) == 0 {
		return nil, firstErr
	}
	return out, nil
}

// groupConsumer returns the cached consumer for (queue, group, member),
// creating it on first use. Idle entries are pruned so churning worker names
// cannot grow the map without bound.
func (c *Client) groupConsumer(queue, group, member string) *Consumer {
	k := queue + "\x00" + group + "\x00" + member
	c.cmu.Lock()
	defer c.cmu.Unlock()
	if w, ok := c.consumers[k]; ok {
		return w
	}
	if len(c.consumers) >= 1024 {
		for key, w := range c.consumers {
			if time.Since(w.lastUsed()) > 3*c.cfg.GroupSession {
				delete(c.consumers, key)
			}
		}
	}
	w := c.NewConsumer(queue, group, member, c.cfg.GroupSession)
	c.consumers[k] = w
	return w
}

// CachedMetadata returns a copy of the metadata last loaded, or nil.
func (c *Client) CachedMetadata() *Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.meta == nil {
		return nil
	}
	m := *c.meta
	return &m
}
