package client_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/pkg/client"
)

func startWorker(t *testing.T, c *client.Client, cfg client.WorkerConfig, h client.Handler) (stop func(), w *client.Worker) {
	t.Helper()
	cfg.Client = c
	if cfg.Queue == "" {
		cfg.Queue = "q"
	}
	if cfg.LongPoll == 0 {
		cfg.LongPoll = 200 * time.Millisecond
	}
	w = client.NewWorker(cfg, h)
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() { w.Run(ctx); close(done) }()
	var once sync.Once
	stop = func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(10 * time.Second):
				t.Error("worker did not stop: goroutine leak")
			}
		})
	}
	t.Cleanup(stop)
	return stop, w
}

func submitN(t *testing.T, c *client.Client, n int) []string {
	t.Helper()
	var ids []string
	for i := 0; i < n; i++ {
		r, err := c.Submit(bg, client.SubmitRequest{Queue: "q", Type: "t", IdempotencyKey: fmt.Sprint("job-", i), Payload: map[string]int{"i": i}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.JobID)
	}
	return ids
}

func waitFor(t *testing.T, what string, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestWorkerProcessesEverythingWithBoundedConcurrency(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	submitN(t, c, 40)
	var cur, peak, done atomic.Int32
	_, w := startWorker(t, c, client.WorkerConfig{Name: "w", Concurrency: 5}, func(ctx context.Context, j client.Job) error {
		n := cur.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		cur.Add(-1)
		done.Add(1)
		return nil
	})
	waitFor(t, "40 jobs processed", 20*time.Second, func() bool { return done.Load() == 40 })
	if p := peak.Load(); p > 5 || p < 3 {
		t.Fatalf("peak concurrency %d; want it to reach the limit of 5 and never exceed it", p)
	}
	waitFor(t, "acks reported", 5*time.Second, func() bool { return w.Stats().Succeeded == 40 })
	if s, _ := c.QueueStats(bg, "q"); s.Total.Acked != 40 || s.Total.Ready+s.Total.InFlight != 0 {
		t.Fatalf("%+v", s.Total)
	}
	if u := w.Stats().Utilization; u <= 0 || u > 1 {
		t.Fatalf("utilization %v", u)
	}
}

func TestWorkerRetriesFailuresThenSucceeds(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 4)
	submitN(t, c, 1)
	var attempts []int
	var mu sync.Mutex
	startWorker(t, c, client.WorkerConfig{Name: "w", Concurrency: 1}, func(ctx context.Context, j client.Job) error {
		mu.Lock()
		attempts = append(attempts, j.Attempt)
		mu.Unlock()
		if j.Attempt < 3 {
			return errors.New("dependency unavailable")
		}
		return nil
	})
	waitFor(t, "3 attempts", 10*time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(attempts) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if fmt.Sprint(attempts) != "[1 2 3]" {
		t.Fatalf("attempts %v", attempts)
	}
}

func TestNonRetryableAndPanicsBehave(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 5)
	submitN(t, c, 3)
	var seen sync.Map
	startWorker(t, c, client.WorkerConfig{Name: "w1", Concurrency: 2}, func(ctx context.Context, j client.Job) error {
		var p struct{ I int }
		_ = p
		seen.Store(j.ID, j.Attempt)
		switch string(j.Payload) {
		case `{"i":0}`:
			return client.NonRetryable(errors.New("malformed payload"))
		case `{"i":1}`:
			panic("nil pointer in handler")
		}
		return nil
	})
	waitFor(t, "one dead from NonRetryable at attempt 1", 15*time.Second, func() bool {
		dl, _ := c.DeadLetters(bg, "q", 10)
		for _, d := range dl {
			if d.LastError == "malformed payload" && d.Attempts == 1 && d.Reason == "non_retryable_error" && d.WorkerID == "w1" {
				return true
			}
		}
		return false
	})
	// The panicking job is retried to exhaustion and then dead-lettered; the worker survives.
	waitFor(t, "panicking job exhausted its attempts", 20*time.Second, func() bool {
		dl, _ := c.DeadLetters(bg, "q", 10)
		for _, d := range dl {
			if d.Attempts == 5 && d.Reason == "max_attempts_exceeded" {
				return true
			}
		}
		return false
	})
	s, _ := c.QueueStats(bg, "q")
	if s.Total.Acked != 1 {
		t.Fatalf("the healthy job should have been acked despite the poison ones: %+v", s.Total)
	}
}

func TestLongJobKeepsItsLeaseThroughRenewal(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	submitN(t, c, 1)
	var runs atomic.Int32
	_, w := startWorker(t, c, client.WorkerConfig{Name: "w", Concurrency: 1, Visibility: 300 * time.Millisecond}, func(ctx context.Context, j client.Job) error {
		runs.Add(1)
		select {
		case <-time.After(1200 * time.Millisecond): // 4x the lease
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	waitFor(t, "handler started", 5*time.Second, func() bool { return runs.Load() == 1 })
	// A second worker would grab the job the moment the lease lapsed.
	rival, _ := c.Pull(bg, "q", client.PullOptions{Worker: "rival", Max: 1, Wait: 1500 * time.Millisecond})
	if len(rival) != 0 {
		t.Fatalf("job was redelivered mid-run: lease renewal failed")
	}
	waitFor(t, "job completed", 5*time.Second, func() bool { return w.Stats().Succeeded == 1 })
	if runs.Load() != 1 || w.Stats().Renewals < 3 {
		t.Fatalf("runs=%d renewals=%d", runs.Load(), w.Stats().Renewals)
	}
}

func TestWorkerAbandonsAJobWhoseLeaseWasLost(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	ids := submitN(t, c, 1)
	started := make(chan struct{})
	var sawCancel atomic.Bool
	_, w := startWorker(t, c, client.WorkerConfig{Name: "w", Concurrency: 1, Visibility: 300 * time.Millisecond}, func(ctx context.Context, j client.Job) error {
		close(started)
		select {
		case <-ctx.Done():
			sawCancel.Store(true)
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})
	<-started
	if err := c.Cancel(bg, ids[0]); err != nil { // someone cancels it: the next renewal gets 404
		t.Fatal(err)
	}
	waitFor(t, "handler told to stop", 5*time.Second, func() bool { return sawCancel.Load() })
	waitFor(t, "lease loss recorded", 5*time.Second, func() bool { return w.Stats().LeaseLost == 1 })
	if w.Stats().Failed != 0 || w.Stats().Succeeded != 0 {
		t.Fatalf("a worker that lost its lease must not report a result: %+v", w.Stats())
	}
}

func TestGracefulShutdownLetsRunningJobsFinish(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	submitN(t, c, 3)
	var started, finished atomic.Int32
	stop, _ := startWorker(t, c, client.WorkerConfig{Name: "w", Concurrency: 3, ShutdownGrace: 5 * time.Second}, func(ctx context.Context, j client.Job) error {
		started.Add(1)
		time.Sleep(400 * time.Millisecond)
		finished.Add(1)
		return nil
	})
	waitFor(t, "all jobs started", 10*time.Second, func() bool { return started.Load() == 3 })
	stop() // SIGTERM equivalent
	if finished.Load() != 3 {
		t.Fatalf("%d of 3 running jobs finished; a graceful stop must let them complete", finished.Load())
	}
	if s, _ := c.QueueStats(bg, "q"); s.Total.Acked != 3 {
		t.Fatalf("results were not reported before exit: %+v", s.Total)
	}
}

func TestForcedShutdownReturnsStuckJobsForQuickRetry(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	ids := submitN(t, c, 1)
	started := make(chan struct{})
	stop, _ := startWorker(t, c, client.WorkerConfig{Name: "w", Concurrency: 1, Visibility: 10 * time.Minute, ShutdownGrace: 200 * time.Millisecond}, func(ctx context.Context, j client.Job) error {
		close(started)
		<-ctx.Done() // a stuck job: only the forced cancel ends it
		return ctx.Err()
	})
	<-started
	begin := time.Now()
	stop()
	if d := time.Since(begin); d > 3*time.Second {
		t.Fatalf("shutdown took %v with a 200ms grace period", d)
	}
	// With a 10-minute lease the job would otherwise be invisible for 10 minutes.
	// It was reported as failed, so it comes back after the retry backoff.
	st, err := c.Get(bg, ids[0])
	if err != nil || st.State != "scheduled" || st.LastError == "" {
		t.Fatalf("job after forced shutdown: %+v %v", st, err)
	}
	t.Logf("stuck job returned to the queue immediately (last error: %q)", st.LastError)
	jobs, err := c.Pull(bg, "q", client.PullOptions{Worker: "next", Max: 1, Wait: 2 * time.Second})
	if err != nil || len(jobs) != 1 || jobs[0].Attempt != 2 {
		t.Fatalf("redelivery: %+v %v", jobs, err)
	}
}

func TestWorkerSurvivesAnUnavailableServer(t *testing.T) {
	c := newServer(t)
	fastQueue(t, c, "q", 3)
	submitN(t, c, 2)
	var calls atomic.Int32
	inner := c.HTTP.Transport
	c.HTTP.Transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) <= 3 {
			return nil, errors.New("connection refused")
		}
		return inner.RoundTrip(r)
	})
	var done atomic.Int32
	_, w := startWorker(t, c, client.WorkerConfig{Name: "w", Concurrency: 1}, func(context.Context, client.Job) error { done.Add(1); return nil })
	waitFor(t, "jobs processed after the outage", 20*time.Second, func() bool { return done.Load() == 2 })
	if w.Stats().PullErrors < 3 {
		t.Fatalf("pull errors = %d", w.Stats().PullErrors)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
