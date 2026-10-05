package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"
)

// Handler processes one job. Return nil to acknowledge it. Return an error to
// have it retried with backoff, or wrap the error with NonRetryable to send
// it straight to the dead-letter queue.
//
// The context is cancelled if the worker loses the job's lease (it was
// redelivered to someone else) or during a forced shutdown. Handlers should
// stop promptly when that happens. Delivery is at-least-once, so a handler
// must be idempotent: the same job can run twice, for example when a worker
// crashes after its side effect but before the acknowledgement.
type Handler func(ctx context.Context, j Job) error

type nonRetryable struct{ err error }

func (e nonRetryable) Error() string { return e.err.Error() }
func (e nonRetryable) Unwrap() error { return e.err }

// NonRetryable marks an error as permanent (malformed input, validation
// failure): retrying cannot help, so the job goes directly to the DLQ.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return nonRetryable{err}
}

// IsNonRetryable reports whether err was marked with NonRetryable.
func IsNonRetryable(err error) bool { var n nonRetryable; return errors.As(err, &n) }

// WorkerConfig configures a Worker.
type WorkerConfig struct {
	Client *Client
	Queue  string
	Group  string // optional consumer group
	Name   string // worker identity, recorded as the lease owner and in the DLQ

	// Concurrency bounds simultaneous handlers (default 4). It is also the
	// bound on prefetch: a worker never leases more jobs than it can run, so
	// a slow worker does not hoard work that others could do.
	Concurrency int
	// Visibility is the lease length requested at pull time (default 30s).
	// RenewEvery (default Visibility/3) is how often a running job's lease is
	// extended, so a short lease detects crashes quickly while long jobs
	// still finish.
	Visibility time.Duration
	RenewEvery time.Duration
	LongPoll   time.Duration // default 20s
	// ShutdownGrace is how long to let running handlers finish after the
	// context is cancelled (default 30s). Afterwards handlers are cancelled
	// and their jobs are reported as failed ("worker shutting down") so they
	// are retried after backoff instead of waiting out the visibility timeout.
	// That failure counts as one attempt.
	ShutdownGrace time.Duration
	Logger        *slog.Logger
}

// WorkerStats is a snapshot of a worker's activity.
type WorkerStats struct {
	Active     int
	Succeeded  uint64
	Failed     uint64 // handler errors reported to the server
	LeaseLost  uint64 // jobs abandoned because the lease was superseded
	Renewals   uint64
	PullErrors uint64
	// Utilization is handler-busy time divided by (Concurrency x uptime).
	Utilization float64
}

// Worker pulls jobs and runs a Handler on them with bounded concurrency.
type Worker struct {
	cfg     WorkerConfig
	handler Handler

	active    atomic.Int64
	succeeded atomic.Uint64
	failed    atomic.Uint64
	lost      atomic.Uint64
	renewals  atomic.Uint64
	pullErrs  atomic.Uint64
	busyNanos atomic.Int64
	started   atomic.Int64 // unix nanos
}

// NewWorker returns a Worker; call Run to start it.
func NewWorker(cfg WorkerConfig, h Handler) *Worker {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.Visibility <= 0 {
		cfg.Visibility = 30 * time.Second
	}
	if cfg.RenewEvery <= 0 {
		cfg.RenewEvery = cfg.Visibility / 3
	}
	if cfg.LongPoll <= 0 {
		cfg.LongPoll = 20 * time.Second
	}
	if cfg.ShutdownGrace <= 0 {
		cfg.ShutdownGrace = 30 * time.Second
	}
	if cfg.Name == "" {
		cfg.Name = fmt.Sprintf("worker-%x", rand.Uint32())
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Worker{cfg: cfg, handler: h}
}

// Stats returns a snapshot.
func (w *Worker) Stats() WorkerStats {
	s := WorkerStats{Active: int(w.active.Load()), Succeeded: w.succeeded.Load(), Failed: w.failed.Load(),
		LeaseLost: w.lost.Load(), Renewals: w.renewals.Load(), PullErrors: w.pullErrs.Load()}
	if st := w.started.Load(); st != 0 {
		up := time.Since(time.Unix(0, st)).Seconds() * float64(w.cfg.Concurrency)
		if up > 0 {
			s.Utilization = min(1, float64(w.busyNanos.Load())/1e9/up)
		}
	}
	return s
}

// Run processes jobs until ctx is cancelled, then drains gracefully:
//  1. stop pulling (no new work);
//  2. let running handlers finish for up to ShutdownGrace;
//  3. cancel the stragglers and report their jobs as failed so they retry soon.
//
// It returns once every handler has returned and every result was reported.
func (w *Worker) Run(ctx context.Context) error {
	w.started.Store(time.Now().UnixNano())
	slots := make(chan struct{}, w.cfg.Concurrency)
	var wg sync.WaitGroup
	hardCtx, hardCancel := context.WithCancel(context.Background())
	defer hardCancel()

	pullBackoff := 100 * time.Millisecond
loop:
	for ctx.Err() == nil {
		// Reserve a slot before pulling so we never hold a lease we cannot run.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		n := 1
	grab:
		for n < cap(slots) {
			select {
			case slots <- struct{}{}:
				n++
			default:
				break grab
			}
		}
		jobs, err := w.cfg.Client.Pull(ctx, w.cfg.Queue, PullOptions{Worker: w.cfg.Name, Group: w.cfg.Group,
			Max: n, Visibility: w.cfg.Visibility, Wait: w.cfg.LongPoll})
		for i := len(jobs); i < n; i++ {
			<-slots
		}
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			w.pullErrs.Add(1)
			w.cfg.Logger.Warn("pull failed", "err", err, "retry_in", pullBackoff)
			d := pullBackoff
			var ae *APIError
			if errors.As(err, &ae) && ae.RetryAfter > d {
				d = ae.RetryAfter
			}
			select {
			case <-time.After(time.Duration(rand.Float64()*float64(d)/2) + d/2): // equal jitter
			case <-ctx.Done():
				break loop
			}
			pullBackoff = min(pullBackoff*2, 5*time.Second)
			continue
		}
		pullBackoff = 100 * time.Millisecond
		for _, j := range jobs {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-slots }()
				w.process(hardCtx, j)
			}()
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(w.cfg.ShutdownGrace):
		w.cfg.Logger.Warn("shutdown grace expired; cancelling running handlers", "active", w.active.Load())
		hardCancel()
		<-done
	}
	return nil
}

func (w *Worker) process(hardCtx context.Context, j Job) {
	w.active.Add(1)
	defer w.active.Add(-1)
	jctx, cancel := context.WithCancel(hardCtx)
	defer cancel()
	var lost atomic.Bool

	renewDone := make(chan struct{})
	go func() {
		defer close(renewDone)
		t := time.NewTicker(w.cfg.RenewEvery)
		defer t.Stop()
		for {
			select {
			case <-t.C:
				rctx, rc := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := w.cfg.Client.Renew(rctx, j.ID, j.Attempt, w.cfg.Visibility)
				rc()
				switch {
				case err == nil:
					w.renewals.Add(1)
				case IsStaleLease(err) || IsNotFound(err):
					// Redelivered to another worker, or cancelled: stop working.
					lost.Store(true)
					cancel()
					return
				default:
					w.cfg.Logger.Warn("lease renewal failed; will retry", "job_id", j.ID, "err", err)
				}
			case <-jctx.Done():
				return
			}
		}
	}()

	start := time.Now()
	err := w.safeCall(jctx, j)
	w.busyNanos.Add(int64(time.Since(start)))
	cancel()
	<-renewDone

	if lost.Load() {
		w.lost.Add(1)
		w.cfg.Logger.Warn("lease lost; abandoning job", "job_id", j.ID, "attempt", j.Attempt)
		return
	}
	rctx, rc := context.WithTimeout(context.Background(), 20*time.Second)
	defer rc()
	if err == nil {
		w.report(rctx, j, func() error { return w.cfg.Client.Ack(rctx, j.ID, j.Attempt) })
		w.succeeded.Add(1)
		return
	}
	msg := err.Error()
	if hardCtx.Err() != nil {
		msg = "worker shutting down: " + msg
	}
	if len(msg) > 1000 {
		msg = msg[:1000]
	}
	w.failed.Add(1)
	w.report(rctx, j, func() error {
		_, e := w.cfg.Client.Fail(rctx, j.ID, j.Attempt, msg, !IsNonRetryable(err))
		return e
	})
}

// report sends a result, retrying transient failures. A stale lease or missing
// job means somebody else already settled the job, which needs no retry.
func (w *Worker) report(ctx context.Context, j Job, send func() error) {
	delay := 100 * time.Millisecond
	for attempt := 1; attempt <= 5; attempt++ {
		err := send()
		if err == nil || IsStaleLease(err) || IsNotFound(err) {
			return
		}
		var ae *APIError
		if errors.As(err, &ae) && !ae.Rejected() && !ae.Unknown() {
			w.cfg.Logger.Error("result rejected", "job_id", j.ID, "err", err)
			return
		}
		select {
		case <-time.After(delay):
			delay *= 2
		case <-ctx.Done():
			return
		}
	}
	w.cfg.Logger.Error("could not report result; the job will be redelivered after its lease expires", "job_id", j.ID)
}

// safeCall runs the handler and converts a panic into an error, so one bad
// job cannot kill the worker or leak its slot.
func (w *Worker) safeCall(ctx context.Context, j Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panic: %v", r)
		}
	}()
	return w.handler(ctx, j)
}
