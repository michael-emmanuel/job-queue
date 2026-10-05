// Package gateway adapts the cluster client to the API's Backend interface,
// so the same public HTTP API fronts either a single-node broker or a
// replicated cluster. A gateway holds no durable state: it caches cluster
// metadata and can be started, stopped or multiplied freely behind a load
// balancer.
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/api"
	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/cluster"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/ratelimit"
	"github.com/michael-emmanuel/jobqueue/internal/replication"
)

type queueLimits struct {
	enq, con *ratelimit.Bucket
	cfg      broker.QueueConfig
}

// Backend implements api.Backend over a cluster.Client.
type Backend struct {
	c   *cluster.Client
	clk clock.Clock

	mu     sync.Mutex
	limits map[string]*queueLimits
}

var _ api.Backend = (*Backend)(nil)

// New returns a Backend.
//
// Per-queue enqueue/consume rate limits are enforced here, in each gateway
// process separately. With N gateways the effective limit is up to N times
// the configured one; exact cluster-wide limits would need a shared counter.
func New(c *cluster.Client, clk clock.Clock) *Backend {
	if clk == nil {
		clk = clock.Real{}
	}
	return &Backend{c: c, clk: clk, limits: map[string]*queueLimits{}}
}

// translate maps cluster failures onto the errors the API understands.
func translate(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, cluster.ErrStaleGeneration):
		return fmt.Errorf("%w: %v", api.ErrStaleGeneration, err)
	case errors.Is(err, replication.ErrLeadershipLost):
		// The write may still commit under the next leader: report it like a
		// timeout, which tells the caller the outcome is unknown.
		return fmt.Errorf("%w: %v", context.DeadlineExceeded, err)
	case errors.Is(err, cluster.ErrNotLeader), errors.Is(err, cluster.ErrNotHosted),
		errors.Is(err, cluster.ErrUnreachable), errors.Is(err, cluster.ErrNoCoordinator),
		errors.Is(err, cluster.ErrNoQuorum), errors.Is(err, cluster.ErrUnauthorized):
		return fmt.Errorf("%w: %v", api.ErrUnavailable, err)
	}
	return err
}

func (b *Backend) limitsFor(ctx context.Context, queue string) (*queueLimits, error) {
	cfg, err := b.c.QueueConfig(ctx, queue)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if l, ok := b.limits[queue]; ok && l.cfg.EnqueueRate == cfg.EnqueueRate && l.cfg.ConsumeRate == cfg.ConsumeRate &&
		l.cfg.EnqueueBurst == cfg.EnqueueBurst && l.cfg.ConsumeBurst == cfg.ConsumeBurst {
		return l, nil
	}
	l := &queueLimits{cfg: cfg,
		enq: ratelimit.NewBucket(b.clk, cfg.EnqueueRate, cfg.EnqueueBurst),
		con: ratelimit.NewBucket(b.clk, cfg.ConsumeRate, cfg.ConsumeBurst)}
	b.limits[queue] = l
	return l, nil
}

func (b *Backend) CreateQueue(ctx context.Context, name string, cfg broker.QueueConfig) (broker.QueueConfig, error) {
	c, err := b.c.CreateQueue(ctx, name, cfg)
	return c, translate(err)
}

func (b *Backend) DeleteQueue(ctx context.Context, name string) error {
	b.mu.Lock()
	delete(b.limits, name)
	b.mu.Unlock()
	return translate(b.c.DeleteQueue(ctx, name))
}

func (b *Backend) ListQueues(ctx context.Context) ([]broker.QueueInfo, error) {
	q, err := b.c.Queues(ctx)
	return q, translate(err)
}

func (b *Backend) QueueStats(ctx context.Context, name string) (broker.QueueStats, error) {
	s, err := b.c.QueueStats(ctx, name)
	return s, translate(err)
}

func (b *Backend) Enqueue(ctx context.Context, queue string, j job.Job) (broker.EnqueueResult, error) {
	l, err := b.limitsFor(ctx, queue)
	if err != nil {
		return broker.EnqueueResult{}, translate(err)
	}
	if ok, wait := l.enq.Take(1); !ok {
		return broker.EnqueueResult{}, &broker.RateLimitedError{Scope: "queue_enqueue", RetryAfter: wait}
	}
	res, err := b.c.Enqueue(ctx, queue, j)
	if err != nil || res.Duplicate {
		l.enq.Refund(1)
	}
	return res, translate(err)
}

func (b *Backend) Pull(ctx context.Context, queue string, a api.PullArgs) ([]job.Job, error) {
	l, err := b.limitsFor(ctx, queue)
	if err != nil {
		return nil, translate(err)
	}
	n := min(max(a.Max, 1), 100)
	if l.con != nil {
		got := l.con.TakeUpTo(n)
		if got == 0 {
			_, wait := l.con.Take(1)
			return nil, &broker.RateLimitedError{Scope: "queue_consume", RetryAfter: wait}
		}
		n = got
	}
	jobs, err := b.c.Pull(ctx, queue, cluster.PullOpts{Group: a.Group, Worker: a.Worker, Max: n,
		Visibility: a.Visibility, Wait: a.Wait})
	if len(jobs) < n {
		l.con.Refund(float64(n - len(jobs)))
	}
	return jobs, translate(err)
}

func (b *Backend) Get(ctx context.Context, id string) (job.Job, error) {
	j, err := b.c.Get(ctx, id)
	return j, translate(err)
}

func (b *Backend) Ack(ctx context.Context, id string, attempt int) error {
	return translate(b.c.Ack(ctx, id, attempt))
}

func (b *Backend) Fail(ctx context.Context, id string, attempt int, f partition.Failure) (partition.Outcome, error) {
	out, err := b.c.Fail(ctx, id, attempt, f.Err, f.NonRetryable)
	return partition.Outcome(out), translate(err)
}

func (b *Backend) Renew(ctx context.Context, id string, attempt int, extend time.Duration) (time.Time, error) {
	t, err := b.c.Renew(ctx, id, attempt, extend)
	return t, translate(err)
}

func (b *Backend) Cancel(ctx context.Context, id string) error { return translate(b.c.Cancel(ctx, id)) }

func (b *Backend) DeadLetters(ctx context.Context, queue string, limit int) ([]job.DeadLetter, error) {
	d, err := b.c.DeadLetters(ctx, queue, limit)
	return d, translate(err)
}

func (b *Backend) ReplayDead(ctx context.Context, id string) error {
	return translate(b.c.ReplayDead(ctx, id))
}

func (b *Backend) DeleteDead(ctx context.Context, id string) error {
	return translate(b.c.DeleteDead(ctx, id))
}

// Ready is true when the coordinator is reachable and metadata loads.
func (b *Backend) Ready() error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.c.Refresh(ctx); err != nil {
		return err
	}
	return nil
}

// Collect reports what the gateway can see of cluster health.
func (b *Backend) Collect(w *observability.Writer) {
	m := b.c.CachedMetadata()
	if m == nil {
		return
	}
	leaderless := 0
	for _, q := range m.Queues {
		for _, p := range q.Partitions {
			if p.Leader == "" {
				leaderless++
			}
		}
	}
	w.Gauge("jobqueue_gateway_queues", "Queues known to this gateway.", float64(len(m.Queues)))
	w.Gauge("jobqueue_gateway_alive_nodes", "Alive nodes in the gateway's last metadata.", float64(len(m.Alive)))
	w.Gauge("jobqueue_gateway_leaderless_partitions", "Partitions without a leader in the gateway's last metadata.", float64(leaderless))
}
