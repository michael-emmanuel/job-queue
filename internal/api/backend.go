package api

import (
	"context"
	"errors"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
)

// Errors a Backend may return in addition to the broker and partition ones.
var (
	// ErrUnavailable means the backend cannot serve the request now (no
	// leader, unreachable node, no quorum). The request was not applied or its
	// outcome is irrelevant to the caller; retrying later is appropriate.
	ErrUnavailable = errors.New("backend unavailable")
	// ErrStaleGeneration means a consumer-group member missed a rebalance.
	ErrStaleGeneration = errors.New("stale consumer group generation")
)

// PullArgs is a worker's request for jobs.
type PullArgs struct {
	Worker     string
	Group      string // optional consumer group (cluster backends only)
	Max        int
	Visibility time.Duration
	Wait       time.Duration
}

// Backend is what the HTTP edge needs from a queue implementation. Two exist:
// the single-node broker (this file) and the cluster client (internal/cluster),
// so the same public API fronts a single process or a replicated cluster.
type Backend interface {
	CreateQueue(ctx context.Context, name string, cfg broker.QueueConfig) (broker.QueueConfig, error)
	DeleteQueue(ctx context.Context, name string) error
	ListQueues(ctx context.Context) ([]broker.QueueInfo, error)
	QueueStats(ctx context.Context, name string) (broker.QueueStats, error)

	Enqueue(ctx context.Context, queue string, j job.Job) (broker.EnqueueResult, error)
	Pull(ctx context.Context, queue string, args PullArgs) ([]job.Job, error)
	Get(ctx context.Context, id string) (job.Job, error)
	Ack(ctx context.Context, id string, attempt int) error
	Fail(ctx context.Context, id string, attempt int, f partition.Failure) (partition.Outcome, error)
	Renew(ctx context.Context, id string, attempt int, extend time.Duration) (time.Time, error)
	Cancel(ctx context.Context, id string) error

	DeadLetters(ctx context.Context, queue string, limit int) ([]job.DeadLetter, error)
	ReplayDead(ctx context.Context, id string) error
	DeleteDead(ctx context.Context, id string) error

	// Ready reports whether the backend can serve traffic.
	Ready() error
	// Collect emits backend-specific metrics.
	Collect(w *observability.Writer)
}

// BrokerBackend adapts the single-node broker to Backend.
func BrokerBackend(b *broker.Broker) Backend { return brokerBackend{b} }

type brokerBackend struct{ b *broker.Broker }

func (x brokerBackend) CreateQueue(_ context.Context, name string, cfg broker.QueueConfig) (broker.QueueConfig, error) {
	return x.b.CreateQueue(name, cfg)
}
func (x brokerBackend) DeleteQueue(_ context.Context, name string) error {
	return x.b.DeleteQueue(name)
}
func (x brokerBackend) ListQueues(context.Context) ([]broker.QueueInfo, error) {
	return x.b.ListQueues(), nil
}
func (x brokerBackend) QueueStats(_ context.Context, name string) (broker.QueueStats, error) {
	q, err := x.b.Queue(name)
	if err != nil {
		return broker.QueueStats{}, err
	}
	return q.Stats(), nil
}
func (x brokerBackend) Enqueue(ctx context.Context, queue string, j job.Job) (broker.EnqueueResult, error) {
	q, err := x.b.Queue(queue)
	if err != nil {
		return broker.EnqueueResult{}, err
	}
	return q.Enqueue(ctx, j)
}

// Pull ignores args.Group: the single-node broker has no consumer groups.
func (x brokerBackend) Pull(ctx context.Context, queue string, a PullArgs) ([]job.Job, error) {
	q, err := x.b.Queue(queue)
	if err != nil {
		return nil, err
	}
	return q.Pull(ctx, broker.PullRequest{Worker: a.Worker, Max: a.Max, Visibility: a.Visibility, Wait: a.Wait})
}
func (x brokerBackend) Get(_ context.Context, id string) (job.Job, error) { return x.b.Get(id) }
func (x brokerBackend) Ack(ctx context.Context, id string, attempt int) error {
	return x.b.Ack(ctx, id, attempt)
}
func (x brokerBackend) Fail(ctx context.Context, id string, attempt int, f partition.Failure) (partition.Outcome, error) {
	return x.b.Fail(ctx, id, attempt, f)
}
func (x brokerBackend) Renew(ctx context.Context, id string, attempt int, extend time.Duration) (time.Time, error) {
	return x.b.Renew(ctx, id, attempt, extend)
}
func (x brokerBackend) Cancel(ctx context.Context, id string) error { return x.b.Cancel(ctx, id) }
func (x brokerBackend) DeadLetters(_ context.Context, queue string, limit int) ([]job.DeadLetter, error) {
	q, err := x.b.Queue(queue)
	if err != nil {
		return nil, err
	}
	return q.DeadLetters(limit), nil
}
func (x brokerBackend) ReplayDead(ctx context.Context, id string) error {
	return x.b.ReplayDead(ctx, id)
}
func (x brokerBackend) DeleteDead(ctx context.Context, id string) error {
	return x.b.DeleteDead(ctx, id)
}
func (x brokerBackend) Ready() error                    { return x.b.Ready() }
func (x brokerBackend) Collect(w *observability.Writer) { x.b.Collect(w) }
