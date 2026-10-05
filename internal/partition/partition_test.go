package partition

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/retry"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

var t0 = time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)

var bg = context.Background()

// testPolicy has no jitter so delays are exact: 1s, 2s, 4s, ... capped at 30s.
var testPolicy = retry.Policy{MaxAttempts: 3, BaseDelay: time.Second, MaxDelay: 30 * time.Second, Jitter: retry.JitterNone}

func newP(t testing.TB, mut ...func(*Config)) (*Partition, *clock.Fake) {
	t.Helper()
	clk := clock.NewFake(t0)
	cfg := Config{Name: "test/0", Log: storage.NewMemLog(), Clock: clk, Retry: testPolicy,
		DefaultVisibility: 10 * time.Second, Rand: func() float64 { return 0.5 }}
	for _, m := range mut {
		m(&cfg)
	}
	p, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p.Close() })
	return p, clk
}

func mk(typ string) job.Job {
	return job.Job{Queue: "q", Type: typ, Payload: json.RawMessage(`{"n":1}`)}
}

func enq(t testing.TB, p *Partition, j job.Job) string {
	t.Helper()
	r, err := p.Enqueue(bg, j)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return r.ID
}

func deq1(t testing.TB, p *Partition, worker string) job.Job {
	t.Helper()
	js, err := p.Dequeue(bg, DequeueRequest{Worker: worker, Max: 1})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if len(js) != 1 {
		t.Fatalf("dequeue returned %d jobs, want 1", len(js))
	}
	return js[0]
}

func expectEmpty(t testing.TB, p *Partition) {
	t.Helper()
	js, err := p.Dequeue(bg, DequeueRequest{Worker: "w", Max: 10})
	if err != nil || len(js) != 0 {
		t.Fatalf("expected empty dequeue, got %d jobs, err=%v", len(js), err)
	}
}

func TestLifecycleEnqueueLeaseAck(t *testing.T) {
	p, _ := newP(t)
	id := enq(t, p, mk("a"))
	if s := p.Stats(); s.Ready != 1 {
		t.Fatalf("ready = %d", s.Ready)
	}
	j := deq1(t, p, "w1")
	if j.ID != id || j.Attempt != 1 || j.State != job.StateInFlight || j.LeaseOwner != "w1" {
		t.Fatalf("unexpected delivery: %+v", j)
	}
	if want := t0.Add(10 * time.Second); !j.VisibleAt.Equal(want) {
		t.Errorf("deadline = %v, want %v", j.VisibleAt, want)
	}
	if err := p.Ack(bg, id, j.Attempt); err != nil {
		t.Fatal(err)
	}
	if s := p.Stats(); s.Ready+s.InFlight+s.Scheduled != 0 || s.Acked != 1 {
		t.Fatalf("stats after ack: %+v", s)
	}
	if _, err := p.Get(id); !errors.Is(err, ErrNotFound) {
		t.Errorf("completed job still present: %v", err)
	}
	if err := p.Ack(bg, id, 1); !errors.Is(err, ErrNotFound) {
		t.Errorf("double ack = %v, want ErrNotFound", err)
	}
}

func TestValidation(t *testing.T) {
	p, _ := newP(t, func(c *Config) { c.MaxPayloadBytes = 16 })
	bad := []job.Job{
		{Type: "t"},
		{Queue: "q"},
		{Queue: "q", Type: "t", Priority: 11},
		{Queue: "q", Type: "t", Payload: json.RawMessage(`{not json`)},
		{Queue: "q", Type: "t", Payload: json.RawMessage(`"0123456789abcdefghij"`)},
	}
	for i, j := range bad {
		if _, err := p.Enqueue(bg, j); !errors.Is(err, ErrInvalidJob) {
			t.Errorf("case %d: err = %v, want ErrInvalidJob", i, err)
		}
	}
}

func TestVisibilityTimeoutRedeliversAndFencesStaleWorker(t *testing.T) {
	p, clk := newP(t)
	id := enq(t, p, mk("a"))
	first := deq1(t, p, "w1")

	clk.Advance(11 * time.Second) // w1 "crashes": lease lapses
	second := deq1(t, p, "w2")
	if second.ID != id || second.Attempt != 2 || second.LeaseOwner != "w2" {
		t.Fatalf("redelivery: %+v", second)
	}
	// The zombie w1 wakes up and tries to ack attempt 1: it must be fenced out.
	if err := p.Ack(bg, id, first.Attempt); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale ack = %v, want ErrStaleLease", err)
	}
	if _, err := p.Fail(bg, id, first.Attempt, Failure{Err: "x"}); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("stale fail = %v, want ErrStaleLease", err)
	}
	if err := p.Ack(bg, id, second.Attempt); err != nil {
		t.Fatalf("current holder ack: %v", err)
	}
}

func TestLateAckAcceptedUntilRedelivered(t *testing.T) {
	p, clk := newP(t)
	id := enq(t, p, mk("a"))
	j := deq1(t, p, "w1")
	clk.Advance(11 * time.Second) // lapsed, but nobody else has leased it
	if err := p.Ack(bg, id, j.Attempt); err != nil {
		t.Fatalf("late ack should succeed while unclaimed: %v", err)
	}
	expectEmpty(t, p)
}

func TestRenewExtendsLeaseAndStopsAfterLapse(t *testing.T) {
	p, clk := newP(t)
	id := enq(t, p, mk("a"))
	j := deq1(t, p, "w1")
	clk.Advance(8 * time.Second)
	dl, err := p.Renew(bg, id, j.Attempt, 10*time.Second)
	if err != nil || !dl.Equal(clk.Now().Add(10*time.Second)) {
		t.Fatalf("renew = %v, %v", dl, err)
	}
	clk.Advance(8 * time.Second) // 16s since lease, but renewed at 8s
	expectEmpty(t, p)            // still invisible
	clk.Advance(3 * time.Second) // renewed lease lapses
	if _, err := p.Renew(bg, id, j.Attempt, time.Second); !errors.Is(err, ErrStaleLease) {
		t.Fatalf("renew after lapse = %v, want ErrStaleLease", err)
	}
	deq1(t, p, "w2")
}

func TestRenewIsCappedAtMaxVisibility(t *testing.T) {
	p, clk := newP(t, func(c *Config) { c.MaxVisibility = time.Minute })
	id := enq(t, p, mk("a"))
	j := deq1(t, p, "w1")
	dl, err := p.Renew(bg, id, j.Attempt, time.Hour)
	if err != nil || !dl.Equal(clk.Now().Add(time.Minute)) {
		t.Fatalf("renew = %v, %v", dl, err)
	}
}

func TestRetryUsesExponentialBackoff(t *testing.T) {
	p, clk := newP(t)
	id := enq(t, p, mk("a"))
	for attempt, wantDelay := range []time.Duration{time.Second, 2 * time.Second} {
		j := deq1(t, p, "w")
		if j.Attempt != attempt+1 {
			t.Fatalf("attempt = %d, want %d", j.Attempt, attempt+1)
		}
		out, err := p.Fail(bg, id, j.Attempt, Failure{Err: "boom"})
		if err != nil || out != OutcomeRetry {
			t.Fatalf("fail = %v, %v", out, err)
		}
		if s := p.Stats(); s.Scheduled != 1 {
			t.Fatalf("scheduled = %d", s.Scheduled)
		}
		clk.Advance(wantDelay - time.Millisecond)
		expectEmpty(t, p)
		clk.Advance(time.Millisecond)
	}
}

func TestJitteredDelaysAreSpread(t *testing.T) {
	// With real jitter, a thousand simultaneous failures must not all retry at
	// the same instant: that is the retry-storm prevention.
	p, _ := newP(t, func(c *Config) {
		c.Retry.Jitter = retry.JitterEqual
		c.Rand = nil
		c.MaxDepth = 2000
	})
	for i := 0; i < 1000; i++ {
		enq(t, p, mk("a"))
	}
	js, _ := p.Dequeue(bg, DequeueRequest{Worker: "w", Max: 100})
	for len(js) > 0 {
		for _, j := range js {
			p.Fail(bg, j.ID, j.Attempt, Failure{Err: "dependency down"})
		}
		js, _ = p.Dequeue(bg, DequeueRequest{Worker: "w", Max: 100})
	}
	p.mu.Lock()
	distinct := map[time.Time]struct{}{}
	for _, j := range p.jobs {
		distinct[j.VisibleAt] = struct{}{}
		if d := j.VisibleAt.Sub(t0); d < 500*time.Millisecond || d > time.Second {
			t.Fatalf("retry delay %v outside equal-jitter window [500ms,1s]", d)
		}
	}
	p.mu.Unlock()
	if len(distinct) < 900 {
		t.Errorf("only %d distinct retry times for 1000 jobs; jitter is not spreading load", len(distinct))
	}
}

func TestDeadLetterAfterMaxAttempts(t *testing.T) {
	p, clk := newP(t)
	id := enq(t, p, mk("a"))
	var out Outcome
	for i := 1; i <= 3; i++ {
		j := deq1(t, p, "worker-"+fmt.Sprint(i))
		var err error
		out, err = p.Fail(bg, id, j.Attempt, Failure{Err: fmt.Sprintf("err%d", i)})
		if err != nil {
			t.Fatal(err)
		}
		clk.Advance(time.Minute)
	}
	if out != OutcomeDead {
		t.Fatalf("final outcome = %v, want dead", out)
	}
	dl := p.DeadLetters(0)
	if len(dl) != 1 {
		t.Fatalf("DLQ has %d entries", len(dl))
	}
	d := dl[0]
	if d.Reason != job.ReasonMaxAttempts || d.Attempts != 3 || d.LastError != "err3" ||
		d.WorkerID != "worker-3" || d.OriginalQueue != "q" || d.OriginalJobID != id {
		t.Errorf("DLQ metadata wrong: %+v", d)
	}
	expectEmpty(t, p)
}

func TestNonRetryableGoesStraightToDLQ(t *testing.T) {
	p, _ := newP(t)
	id := enq(t, p, mk("a"))
	j := deq1(t, p, "w")
	out, err := p.Fail(bg, id, j.Attempt, Failure{Err: "malformed", NonRetryable: true})
	if err != nil || out != OutcomeDead {
		t.Fatalf("fail = %v, %v", out, err)
	}
	if d := p.DeadLetters(0); len(d) != 1 || d[0].Reason != job.ReasonNonRetryable || d[0].Attempts != 1 {
		t.Fatalf("DLQ: %+v", d)
	}
}

// A poison message that kills its worker never reports a failure; only the
// visibility timeout can detect it, and it must still end in the DLQ.
func TestPoisonJobThatCrashesWorkersIsDeadLettered(t *testing.T) {
	p, clk := newP(t)
	enq(t, p, mk("poison"))
	for i := 0; i < 3; i++ {
		deq1(t, p, "doomed")
		clk.Advance(11 * time.Second)
	}
	expectEmpty(t, p)
	d := p.DeadLetters(0)
	if len(d) != 1 || d[0].Reason != job.ReasonVisibilityExhausted || d[0].Attempts != 3 {
		t.Fatalf("DLQ: %+v", d)
	}
}

func TestDLQReplayAndDelete(t *testing.T) {
	p, _ := newP(t)
	a, b := enq(t, p, mk("a")), enq(t, p, mk("b"))
	for i := 0; i < 2; i++ {
		j := deq1(t, p, "w")
		p.Fail(bg, j.ID, j.Attempt, Failure{NonRetryable: true, Err: "bad"})
	}
	if s := p.Stats(); s.Dead != 2 {
		t.Fatalf("dead = %d", s.Dead)
	}
	if err := p.ReplayDead(bg, a); err != nil {
		t.Fatal(err)
	}
	j := deq1(t, p, "w2")
	if j.ID != a || j.Attempt != 1 {
		t.Fatalf("replayed job: %+v (attempt must reset)", j)
	}
	if err := p.DeleteDead(bg, b); err != nil {
		t.Fatal(err)
	}
	if err := p.DeleteDead(bg, b); !errors.Is(err, ErrNotFound) {
		t.Errorf("second delete = %v", err)
	}
	if s := p.Stats(); s.Dead != 0 {
		t.Errorf("dead = %d", s.Dead)
	}
}

func TestScheduledJobIsInvisibleUntilRunAt(t *testing.T) {
	p, clk := newP(t)
	j := mk("later")
	j.RunAt = t0.Add(time.Hour)
	id := enq(t, p, j)
	if s := p.Stats(); s.Scheduled != 1 || s.Ready != 0 {
		t.Fatalf("stats: %+v", s)
	}
	expectEmpty(t, p)
	clk.Advance(59 * time.Minute)
	expectEmpty(t, p)
	clk.Advance(time.Minute)
	if got := deq1(t, p, "w"); got.ID != id {
		t.Fatalf("got %s", got.ID)
	}
}

func TestIdempotencyKeyDeduplicates(t *testing.T) {
	p, clk := newP(t, func(c *Config) { c.IdempotencyTTL = time.Hour })
	j := mk("a")
	j.IdempotencyKey = "signup-123"
	first, err := p.Enqueue(bg, j)
	if err != nil || first.Duplicate {
		t.Fatalf("first: %+v %v", first, err)
	}
	second, _ := p.Enqueue(bg, j)
	if !second.Duplicate || second.ID != first.ID {
		t.Fatalf("second: %+v, want duplicate of %s", second, first.ID)
	}
	other := j
	other.Tenant = "acme" // same key, different tenant: independent namespace
	if r, _ := p.Enqueue(bg, other); r.Duplicate {
		t.Fatal("idempotency keys must be scoped per tenant")
	}
	if s := p.Stats(); s.Ready != 2 || s.Duplicates != 1 {
		t.Fatalf("stats: %+v", s)
	}

	// The key must outlive the job: a client retry after completion must not re-run it.
	got := deq1(t, p, "w")
	p.Ack(bg, got.ID, got.Attempt)
	if r, _ := p.Enqueue(bg, j); !r.Duplicate {
		t.Fatal("key forgotten after job completed")
	}
	clk.Advance(2 * time.Hour) // TTL expired: key is reusable
	if r, _ := p.Enqueue(bg, j); r.Duplicate {
		t.Fatal("key should have expired")
	}
}

func TestWeightedFairnessDoesNotStarveLowPriority(t *testing.T) {
	p, _ := newP(t)
	for i := 0; i < 70; i++ {
		for _, prio := range []int{10, 5, 1} {
			j := mk("a")
			j.Priority = prio
			enq(t, p, j)
		}
	}
	counts := map[int]int{}
	js, _ := p.Dequeue(bg, DequeueRequest{Worker: "w", Max: 70})
	for _, j := range js {
		counts[j.Priority]++
	}
	// 70 dequeues = 10 full cycles of weights 4:2:1.
	if counts[10] != 40 || counts[5] != 20 || counts[1] != 10 {
		t.Fatalf("service split high/normal/low = %d/%d/%d, want 40/20/10", counts[10], counts[5], counts[1])
	}
}

func TestSchedulerIsWorkConserving(t *testing.T) {
	p, _ := newP(t)
	for i := 0; i < 20; i++ {
		j := mk("a")
		j.Priority = 1
		enq(t, p, j)
	}
	js, _ := p.Dequeue(bg, DequeueRequest{Worker: "w", Max: 20})
	if len(js) != 20 {
		t.Fatalf("low-priority-only queue served %d/20; weights must not idle capacity", len(js))
	}
}

func TestBackpressureRejectsAtMaxDepth(t *testing.T) {
	p, _ := newP(t, func(c *Config) { c.MaxDepth = 3 })
	for i := 0; i < 3; i++ {
		enq(t, p, mk("a"))
	}
	if _, err := p.Enqueue(bg, mk("a")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	j := deq1(t, p, "w") // in-flight still counts as backlog
	if _, err := p.Enqueue(bg, mk("a")); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("in-flight must count toward depth; err = %v", err)
	}
	p.Ack(bg, j.ID, j.Attempt)
	enq(t, p, mk("a")) // capacity freed
	if s := p.Stats(); s.Rejected != 2 {
		t.Errorf("rejected = %d", s.Rejected)
	}
}

func TestCancel(t *testing.T) {
	p, _ := newP(t)
	a := enq(t, p, mk("a"))
	b := enq(t, p, mk("b"))
	if err := p.Cancel(bg, a); err != nil {
		t.Fatal(err)
	}
	j := deq1(t, p, "w") // cancelled job's stale queue entry must be skipped
	if j.ID != b {
		t.Fatalf("got %s, want %s", j.ID, b)
	}
	if err := p.Cancel(bg, b); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Renew(bg, b, j.Attempt, time.Second); !errors.Is(err, ErrNotFound) {
		t.Fatalf("renew after cancel = %v; the worker must learn the job is gone", err)
	}
}

func TestLongPollWakesOnEnqueue(t *testing.T) {
	p, _ := newP(t, func(c *Config) { c.Clock = clock.Real{} })
	got := make(chan []job.Job, 1)
	go func() {
		js, _ := p.Dequeue(bg, DequeueRequest{Worker: "w", Wait: 5 * time.Second})
		got <- js
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	enq(t, p, mk("a"))
	select {
	case js := <-got:
		if len(js) != 1 {
			t.Fatalf("got %d jobs", len(js))
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("wake took %v", d)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("long poll did not wake on enqueue")
	}
}

func TestLongPollTimesOutEmpty(t *testing.T) {
	p, _ := newP(t)
	start := time.Now()
	js, err := p.Dequeue(bg, DequeueRequest{Worker: "w", Wait: 80 * time.Millisecond})
	if err != nil || len(js) != 0 || time.Since(start) < 70*time.Millisecond {
		t.Fatalf("js=%v err=%v after %v", js, err, time.Since(start))
	}
}

func TestLongPollRespectsContext(t *testing.T) {
	p, _ := newP(t)
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	if _, err := p.Dequeue(ctx, DequeueRequest{Worker: "w", Wait: time.Minute}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func TestCloseWakesLongPollers(t *testing.T) {
	p, _ := newP(t)
	errc := make(chan error, 1)
	go func() {
		_, err := p.Dequeue(bg, DequeueRequest{Worker: "w", Wait: time.Minute})
		errc <- err
	}()
	time.Sleep(30 * time.Millisecond)
	p.Close()
	select {
	case err := <-errc:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not release the blocked consumer: graceful shutdown would hang")
	}
}

func TestRunLoopPromotesScheduledJobsAndWakesPollers(t *testing.T) {
	p, _ := newP(t, func(c *Config) { c.Clock = clock.Real{} })
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	defer func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Run did not stop on context cancellation: goroutine leak")
		}
	}()
	j := mk("a")
	j.RunAt = time.Now().Add(100 * time.Millisecond)
	enq(t, p, j)
	start := time.Now()
	js, err := p.Dequeue(bg, DequeueRequest{Worker: "w", Wait: 3 * time.Second})
	if err != nil || len(js) != 1 {
		t.Fatalf("js=%v err=%v", js, err)
	}
	if d := time.Since(start); d < 80*time.Millisecond || d > time.Second {
		t.Errorf("scheduled job delivered after %v, want ~100ms", d)
	}
}

func TestConcurrentProducersAndConsumers(t *testing.T) {
	const producers, perProducer, consumers = 8, 500, 8
	const total = producers * perProducer
	p, _ := newP(t, func(c *Config) {
		c.Clock = clock.Real{}
		c.MaxDepth = total
		c.DefaultVisibility = time.Minute
	})
	ctx, cancel := context.WithTimeout(bg, 30*time.Second)
	defer cancel()

	var pw sync.WaitGroup
	for i := 0; i < producers; i++ {
		pw.Add(1)
		go func(i int) {
			defer pw.Done()
			for k := 0; k < perProducer; k++ {
				j := mk("a")
				j.Priority = 1 + (i+k)%10
				if _, err := p.Enqueue(ctx, j); err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(i)
	}

	var processed atomic.Int64
	seen := make(map[string]int)
	var mu sync.Mutex
	var cw sync.WaitGroup
	for c := 0; c < consumers; c++ {
		cw.Add(1)
		go func(c int) {
			defer cw.Done()
			for processed.Load() < total && ctx.Err() == nil {
				js, err := p.Dequeue(ctx, DequeueRequest{Worker: fmt.Sprint("w", c), Max: 10, Wait: 20 * time.Millisecond})
				if err != nil {
					return
				}
				for _, j := range js {
					if err := p.Ack(ctx, j.ID, j.Attempt); err != nil {
						t.Errorf("ack: %v", err)
						return
					}
					mu.Lock()
					seen[j.ID]++
					mu.Unlock()
					processed.Add(1)
				}
			}
		}(c)
	}
	pw.Wait()
	cw.Wait()
	if len(seen) != total {
		t.Fatalf("processed %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s delivered %d times with no lease expiry", id, n)
		}
	}
	if s := p.Stats(); s.Ready+s.InFlight+s.Scheduled != 0 {
		t.Fatalf("leftover: %+v", s)
	}
}

func BenchmarkEnqueueDequeueAckMemLog(b *testing.B) {
	p, _ := newP(b, func(c *Config) { c.Clock = clock.Real{}; c.MaxDepth = 1 << 30 })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r, _ := p.Enqueue(bg, mk("a"))
		js, _ := p.Dequeue(bg, DequeueRequest{Worker: "w"})
		p.Ack(bg, r.ID, js[0].Attempt)
	}
}

// gatedLog is a Log whose durability waits block until released: a stand-in
// for a slow fsync or a replication quorum that has not answered yet.
type gatedLog struct {
	*storage.MemLog
	gate chan struct{}
}

func (g *gatedLog) Append(r storage.Record) (storage.WaitFunc, error) {
	if _, err := g.MemLog.Append(r); err != nil {
		return nil, err
	}
	return func(ctx context.Context) error {
		select {
		case <-g.gate:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}, nil
}

// An idempotent retry must never be acknowledged on the strength of state that
// is not yet durable. Otherwise a client whose first attempt timed out could
// be told "duplicate, already accepted" for a write that a crash then erases.
func TestDuplicateIsNotAcknowledgedBeforeTheOriginalIsDurable(t *testing.T) {
	g := &gatedLog{MemLog: storage.NewMemLog(), gate: make(chan struct{})}
	p, _ := newP(t, func(c *Config) { c.Log = g })
	j := mk("a")
	j.IdempotencyKey = "k"

	type res struct {
		r   EnqueueResult
		err error
	}
	first, second := make(chan res, 1), make(chan res, 1)
	go func() { r, err := p.Enqueue(bg, j); first <- res{r, err} }()
	// Wait until the first enqueue has been applied (visible in memory) but not made durable.
	for p.Stats().Enqueued == 0 {
		time.Sleep(time.Millisecond)
	}
	go func() { r, err := p.Enqueue(bg, j); second <- res{r, err} }()

	select {
	case r := <-second:
		t.Fatalf("duplicate acknowledged (%+v, %v) while the original was still not durable", r.r, r.err)
	case <-time.After(150 * time.Millisecond):
	}

	// A caller that gives up gets an error, not a success.
	ctx, cancel := context.WithTimeout(bg, 50*time.Millisecond)
	defer cancel()
	if _, err := p.Enqueue(ctx, j); err == nil {
		t.Fatal("a duplicate whose original is not durable returned success to a caller that timed out")
	}

	close(g.gate) // durability arrives
	for i, ch := range []chan res{first, second} {
		select {
		case r := <-ch:
			if r.err != nil {
				t.Fatalf("enqueue %d: %v", i, r.err)
			}
			if i == 1 && !r.r.Duplicate {
				t.Fatal("second call should have been reported as a duplicate")
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("enqueue %d never completed after durability arrived", i)
		}
	}
}
