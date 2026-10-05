package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/routing"
)

var bg = context.Background()

func newBroker(t testing.TB, mut ...func(*Config)) *Broker {
	t.Helper()
	cfg := Config{}
	for _, m := range mut {
		m(&cfg)
	}
	b, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func mkQueue(t testing.TB, b *Broker, name string, cfg QueueConfig) *Queue {
	t.Helper()
	if _, err := b.CreateQueue(name, cfg); err != nil {
		t.Fatal(err)
	}
	q, err := b.Queue(name)
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func jb(key string) job.Job { return job.Job{Type: "t", PartitionKey: key} }

func TestJobIDsRouteThemselves(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "email", QueueConfig{Partitions: 4})
	r, err := q.Enqueue(bg, jb("user-1"))
	if err != nil {
		t.Fatal(err)
	}
	ref, err := b.Locate(r.ID)
	if err != nil || ref.Queue != "email" || ref.Partition != r.Partition {
		t.Fatalf("locate(%s) = %+v, %v", r.ID, ref, err)
	}
	if got, err := b.Get(r.ID); err != nil || got.ID != r.ID || got.Queue != "email" {
		t.Fatalf("get: %+v %v", got, err)
	}
	for _, bad := range []string{"", "nodots", "email.x.abc", "email.99.abc", "Email.0.abc", "email.-1.abc", "email.0."} {
		if _, err := b.Get(bad); err == nil {
			t.Errorf("id %q accepted", bad)
		}
	}
	if _, err := b.Get("ghost.0.abc"); !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("unknown queue = %v", err)
	}
}

func TestSamePartitionKeyLandsOnOnePartition(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 8})
	seen := map[int]bool{}
	for i := 0; i < 50; i++ {
		r, _ := q.Enqueue(bg, jb("merchant-7"))
		seen[r.Partition] = true
	}
	if len(seen) != 1 {
		t.Fatalf("one key spread over %d partitions", len(seen))
	}
}

func TestKeylessJobsSpreadAcrossPartitions(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 4})
	for i := 0; i < 400; i++ {
		q.Enqueue(bg, jb(""))
	}
	for i, p := range q.Stats().Partitions {
		if p.Ready != 100 {
			t.Errorf("partition %d holds %d jobs, want 100", i, p.Ready)
		}
	}
}

func TestPullDrainsEveryPartition(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 6})
	want := map[string]bool{}
	for i := 0; i < 120; i++ {
		r, err := q.Enqueue(bg, jb(fmt.Sprint("k", i)))
		if err != nil {
			t.Fatal(err)
		}
		want[r.ID] = true
	}
	for len(want) > 0 {
		js, err := q.Pull(bg, PullRequest{Worker: "w", Max: 7})
		if err != nil || len(js) == 0 {
			t.Fatalf("pull: %d jobs, %v; %d outstanding", len(js), err, len(want))
		}
		for _, j := range js {
			if !want[j.ID] {
				t.Fatalf("unexpected or duplicate job %s", j.ID)
			}
			delete(want, j.ID)
			if err := b.Ack(bg, j.ID, j.Attempt); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestLongPollWakesWhenAnyPartitionGetsWork(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 8})
	got := make(chan []job.Job, 1)
	go func() {
		js, _ := q.Pull(bg, PullRequest{Worker: "w", Max: 1, Wait: 5 * time.Second})
		got <- js
	}()
	time.Sleep(50 * time.Millisecond)
	q.Enqueue(bg, jb("some-key"))
	select {
	case js := <-got:
		if len(js) != 1 {
			t.Fatalf("got %d jobs", len(js))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("poller missed the wakeup")
	}
}

// With a queue-wide limit split across partitions, a hot key exhausts its
// partition's share long before the queue as a whole is full.
func TestHotPartitionRejectsBeforeQueueIsFull(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "payments", QueueConfig{Partitions: 4, MaxDepth: 100}) // 25 per partition
	accepted, rejected := 0, 0
	for i := 0; i < 60; i++ {
		_, err := q.Enqueue(bg, jb("merchant-hot"))
		switch {
		case err == nil:
			accepted++
		case errors.Is(err, partition.ErrQueueFull):
			rejected++
		default:
			t.Fatal(err)
		}
	}
	if accepted != 25 || rejected != 35 {
		t.Fatalf("accepted %d rejected %d, want 25/35", accepted, rejected)
	}
	if total := q.Stats().Total.Ready; total != 25 {
		t.Fatalf("queue holds %d jobs of a 100 limit: the other partitions sat idle", total)
	}
	// A key that routes to a different partition is unaffected.
	hot := routing.Partition("payments", "merchant-hot", 4)
	for i := 0; ; i++ {
		k := fmt.Sprint("merchant-", i)
		if routing.Partition("payments", k, 4) == hot {
			continue
		}
		if _, err := q.Enqueue(bg, jb(k)); err != nil {
			t.Fatalf("cold partition rejected a job: %v", err)
		}
		break
	}
}

func TestIdempotentRetryReachesTheSamePartition(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 8})
	j := job.Job{Type: "t", IdempotencyKey: "signup-123"}
	first, err := q.Enqueue(bg, j)
	if err != nil || first.Duplicate {
		t.Fatalf("%+v %v", first, err)
	}
	for i := 0; i < 5; i++ {
		again, err := q.Enqueue(bg, j)
		if err != nil || !again.Duplicate || again.ID != first.ID {
			t.Fatalf("retry %d: %+v %v, want duplicate of %s", i, again, err, first.ID)
		}
	}
	if q.Stats().Total.Ready != 1 {
		t.Fatal("duplicates created extra jobs")
	}
}

func TestEnqueueRateLimitRefundsRejectedAndDuplicateRequests(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	b := newBroker(t, func(c *Config) { c.Clock = clk })
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 1, EnqueueRate: 1, EnqueueBurst: 2, MaxDepth: 1})
	if _, err := q.Enqueue(bg, jb("a")); err != nil {
		t.Fatal(err)
	}
	// Queue is now full: these rejections must not drain the rate budget.
	for i := 0; i < 5; i++ {
		if _, err := q.Enqueue(bg, jb("a")); !errors.Is(err, partition.ErrQueueFull) {
			t.Fatalf("err = %v", err)
		}
	}
	js, _ := q.Pull(bg, PullRequest{Worker: "w"})
	b.Ack(bg, js[0].ID, js[0].Attempt)
	if _, err := q.Enqueue(bg, jb("a")); err != nil {
		t.Fatalf("budget was consumed by rejected requests: %v", err)
	}
	var rl *RateLimitedError
	if _, err := q.Enqueue(bg, jb("a")); !errors.As(err, &rl) || rl.RetryAfter <= 0 {
		t.Fatalf("err = %v, want RateLimitedError with RetryAfter", err)
	}
}

func TestConsumeRateLimit(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	b := newBroker(t, func(c *Config) { c.Clock = clk })
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 2, ConsumeRate: 10, ConsumeBurst: 4})
	for i := 0; i < 20; i++ {
		q.Enqueue(bg, jb(fmt.Sprint(i)))
	}
	js, err := q.Pull(bg, PullRequest{Worker: "w", Max: 10})
	if err != nil || len(js) != 4 {
		t.Fatalf("got %d jobs (%v), want burst of 4", len(js), err)
	}
	var rl *RateLimitedError
	if _, err := q.Pull(bg, PullRequest{Worker: "w", Max: 10}); !errors.As(err, &rl) {
		t.Fatalf("err = %v, want RateLimitedError", err)
	}
	clk.Advance(300 * time.Millisecond) // 3 tokens
	js, _ = q.Pull(bg, PullRequest{Worker: "w", Max: 10})
	if len(js) != 3 {
		t.Fatalf("got %d after refill, want 3", len(js))
	}
}

func TestEmptyPullRefundsConsumeTokens(t *testing.T) {
	clk := clock.NewFake(time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC))
	b := newBroker(t, func(c *Config) { c.Clock = clk })
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 1, ConsumeRate: 1, ConsumeBurst: 5})
	for i := 0; i < 50; i++ { // idle polling must not drain the budget
		if js, err := q.Pull(bg, PullRequest{Worker: "w", Max: 5}); err != nil || len(js) != 0 {
			t.Fatalf("poll %d: %v %v", i, js, err)
		}
	}
	q.Enqueue(bg, jb("x"))
	if js, err := q.Pull(bg, PullRequest{Worker: "w", Max: 5}); err != nil || len(js) != 1 {
		t.Fatalf("job unavailable after idle polling: %v %v", js, err)
	}
}

func TestQueueLifecycleAndValidation(t *testing.T) {
	b := newBroker(t)
	for _, bad := range []string{"", "Upper", "has.dot", "-lead", "sp ace", string(make([]byte, 64))} {
		if _, err := b.CreateQueue(bad, QueueConfig{}); !errors.Is(err, ErrInvalidQueue) {
			t.Errorf("name %q: %v", bad, err)
		}
	}
	if _, err := b.CreateQueue("q", QueueConfig{Partitions: 1000}); !errors.Is(err, ErrInvalidQueue) {
		t.Errorf("partitions: %v", err)
	}
	if _, err := b.CreateQueue("q", QueueConfig{Retry: RetryConfig{Jitter: "wild"}}); !errors.Is(err, ErrInvalidQueue) {
		t.Errorf("jitter: %v", err)
	}
	cfg, err := b.CreateQueue("q", QueueConfig{Partitions: 2})
	if err != nil || cfg.DefaultVisibility.Std() != 30*time.Second || cfg.Retry.MaxAttempts != 5 {
		t.Fatalf("defaults not applied: %+v %v", cfg, err)
	}
	if _, err := b.CreateQueue("q", QueueConfig{}); !errors.Is(err, ErrQueueExists) {
		t.Errorf("duplicate create: %v", err)
	}
	if _, err := b.Queue("missing"); !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("missing queue: %v (must not auto-create by default)", err)
	}
	if got := b.ListQueues(); len(got) != 1 || got[0].Name != "q" {
		t.Errorf("list: %+v", got)
	}
	if err := b.DeleteQueue("q"); err != nil {
		t.Fatal(err)
	}
	if err := b.DeleteQueue("q"); !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("second delete: %v", err)
	}
}

func TestAutoCreate(t *testing.T) {
	b := newBroker(t, func(c *Config) { c.AutoCreate = true })
	if _, err := b.Queue("fresh"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Queue("Not Valid"); err == nil {
		t.Fatal("auto-create accepted an invalid name")
	}
}

func persistent(t testing.TB, dir string) *Broker {
	t.Helper()
	b, err := Open(Config{DataDir: dir, SegmentBytes: 8 << 10})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestQueuesAndJobsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	b := persistent(t, dir)
	q := mkQueue(t, b, "orders", QueueConfig{Partitions: 3, MaxDepth: 300})
	ids := map[string]bool{}
	for i := 0; i < 30; i++ {
		r, err := q.Enqueue(bg, job.Job{Type: "t", PartitionKey: fmt.Sprint(i), IdempotencyKey: fmt.Sprint("k", i)})
		if err != nil {
			t.Fatal(err)
		}
		ids[r.ID] = true
	}
	js, _ := q.Pull(bg, PullRequest{Worker: "w", Max: 5})
	for _, j := range js[:2] {
		b.Ack(bg, j.ID, j.Attempt)
		delete(ids, j.ID)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}

	b2 := persistent(t, dir)
	defer b2.Close()
	q2, err := b2.Queue("orders")
	if err != nil {
		t.Fatalf("queue lost across restart: %v", err)
	}
	if q2.Config().Partitions != 3 || q2.Config().MaxDepth != 300 {
		t.Errorf("config not restored: %+v", q2.Config())
	}
	if _, err := os.Stat(filepath.Join(dir, "queues.json")); err != nil {
		t.Error(err)
	}
	// 3 jobs were in flight with a 30s lease: invisible. 25 remain ready.
	if s := q2.Stats().Total; s.Ready != 25 || s.InFlight != 3 {
		t.Fatalf("after restart: %+v", s)
	}
	r, _ := q2.Enqueue(bg, job.Job{Type: "t", PartitionKey: "5", IdempotencyKey: "k5"})
	if !r.Duplicate {
		t.Error("idempotency state lost across restart")
	}
}

func TestDeletedQueueDoesNotResurrectItsJobs(t *testing.T) {
	dir := t.TempDir()
	b := persistent(t, dir)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 2})
	for i := 0; i < 10; i++ {
		q.Enqueue(bg, jb(fmt.Sprint(i)))
	}
	if err := b.DeleteQueue("q"); err != nil {
		t.Fatal(err)
	}
	q2 := mkQueue(t, b, "q", QueueConfig{Partitions: 2})
	if s := q2.Stats().Total; s.Ready != 0 {
		t.Fatalf("recreated queue holds %d old jobs", s.Ready)
	}
	b.Close()
}

func TestCrashBeforeDirectoryCleanupDoesNotResurrectJobs(t *testing.T) {
	dir := t.TempDir()
	b := persistent(t, dir)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 1})
	for i := 0; i < 5; i++ {
		q.Enqueue(bg, jb("k"))
	}
	b.Close()
	// Simulate DeleteQueue crashing after deregistering but before RemoveAll:
	// metadata says the queue is gone, its directory is still there.
	if err := os.WriteFile(filepath.Join(dir, "queues.json"), []byte(`{"version":1,"queues":{}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	b2 := persistent(t, dir)
	defer b2.Close()
	q2 := mkQueue(t, b2, "q", QueueConfig{Partitions: 1})
	if s := q2.Stats().Total; s.Ready != 0 {
		t.Fatalf("orphan directory resurrected %d jobs", s.Ready)
	}
}

func TestReadinessReflectsBrokerState(t *testing.T) {
	b := newBroker(t)
	if err := b.Ready(); err != nil {
		t.Fatal(err)
	}
	mkQueue(t, b, "q", QueueConfig{Partitions: 1})
	if err := b.Ready(); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if err := b.Ready(); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed broker ready = %v", err)
	}
}

func TestCloseReleasesBlockedPollers(t *testing.T) {
	b := newBroker(t)
	q := mkQueue(t, b, "q", QueueConfig{Partitions: 2})
	errc := make(chan error, 1)
	go func() {
		_, err := q.Pull(bg, PullRequest{Worker: "w", Wait: time.Minute})
		errc <- err
	}()
	time.Sleep(50 * time.Millisecond)
	if err := b.DeleteQueue("q"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-errc:
	case <-time.After(3 * time.Second):
		t.Fatal("poller still blocked after queue deletion")
	}
}
