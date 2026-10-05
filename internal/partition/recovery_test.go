package partition

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

func openWAL(t testing.TB, dir string, clk clock.Clock, mut ...func(*Config)) (*Partition, *storage.WAL) {
	t.Helper()
	w, err := storage.Open(storage.Options{Dir: dir, SegmentBytes: 4 << 10})
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Name: "wal/0", Log: w, Clock: clk, Retry: testPolicy,
		DefaultVisibility: 10 * time.Second, Rand: func() float64 { return 0.5 }}
	for _, m := range mut {
		m(&cfg)
	}
	p, err := Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return p, w
}

// view is a comparable summary of one job's canonical state.
func view(p *Partition, now time.Time) map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.advanceLocked(now)
	out := map[string]string{}
	for id, j := range p.jobs {
		out[id] = fmt.Sprintf("%s a=%d owner=%q visible=%s", j.State, j.Attempt, j.LeaseOwner, j.VisibleAt.UTC().Format(time.RFC3339Nano))
	}
	for id, d := range p.dead {
		out["dlq:"+id] = fmt.Sprintf("%s a=%d", d.Reason, d.Attempts)
	}
	return out
}

func sameView(t *testing.T, label string, a, b map[string]string) {
	t.Helper()
	if len(a) != len(b) {
		t.Fatalf("%s: %d entries vs %d", label, len(a), len(b))
	}
	for k, v := range a {
		if b[k] != v {
			t.Fatalf("%s: %s\n  live:      %s\n  recovered: %s", label, k, v, b[k])
		}
	}
}

func TestCrashRecoveryKeepsAcknowledgedWorkAndLeases(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(t0)
	p, w := openWAL(t, dir, clk)

	var ids []string
	for i := 0; i < 6; i++ {
		j := mk(fmt.Sprint("t", i))
		j.IdempotencyKey = fmt.Sprint("key", i)
		ids = append(ids, enq(t, p, j))
	}
	later := mk("later")
	later.RunAt = t0.Add(time.Hour)
	laterID := enq(t, p, later)

	a := deq1(t, p, "w1") // will be acked
	b := deq1(t, p, "w2") // will fail -> retry scheduled
	c := deq1(t, p, "w3") // stays in flight across the crash
	if err := p.Ack(bg, a.ID, a.Attempt); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Fail(bg, b.ID, b.Attempt, Failure{Err: "boom"}); err != nil {
		t.Fatal(err)
	}
	d := deq1(t, p, "w4")
	if _, err := p.Fail(bg, d.ID, d.Attempt, Failure{NonRetryable: true, Err: "bad"}); err != nil {
		t.Fatal(err)
	}

	before := view(p, clk.Now())
	w.Abort() // process killed: no flush, no graceful close

	p2, _ := openWAL(t, dir, clk)
	defer p2.Close()
	sameView(t, "after crash", before, view(p2, clk.Now()))

	if err := p2.Ack(bg, a.ID, a.Attempt); err == nil {
		t.Error("acked job came back after crash: acknowledged work was lost")
	}
	// The surviving lease is honoured: the original worker can still finish.
	if err := p2.Ack(bg, c.ID, c.Attempt); err != nil {
		t.Errorf("in-flight lease not preserved across crash: %v", err)
	}
	// Idempotency keys survive restart.
	dup := mk("t0")
	dup.IdempotencyKey = "key0"
	if r, _ := p2.Enqueue(bg, dup); !r.Duplicate || r.ID != ids[0] {
		t.Errorf("idempotency key lost in crash: %+v", r)
	}
	// The scheduled job is still scheduled, and the DLQ entry survived.
	clk.Advance(2 * time.Hour)
	p2.mu.Lock()
	p2.advanceLocked(clk.Now())
	st := p2.jobs[laterID].State
	p2.mu.Unlock()
	if st != job.StateReady {
		t.Errorf("scheduled job state after recovery+time = %v", st)
	}
	if dl := p2.DeadLetters(0); len(dl) != 1 || dl[0].OriginalJobID != d.ID {
		t.Errorf("DLQ after crash: %+v", dl)
	}
}

func TestInFlightJobsAreRedeliveredAfterLongOutage(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(t0)
	p, w := openWAL(t, dir, clk)
	id := enq(t, p, mk("a"))
	first := deq1(t, p, "w1")
	w.Abort()

	clk.Advance(time.Hour) // broker was down far longer than the visibility timeout
	p2, _ := openWAL(t, dir, clk)
	defer p2.Close()
	second := deq1(t, p2, "w2")
	if second.ID != id || second.Attempt != 2 {
		t.Fatalf("redelivery: %+v", second)
	}
	if err := p2.Ack(bg, id, first.Attempt); err == nil {
		t.Fatal("pre-crash worker must be fenced after redelivery")
	}
}

func TestCompactionShrinksLogAndPreservesState(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewFake(t0)
	p, _ := openWAL(t, dir, clk)

	// 2000 jobs churn through; 20 remain. Without compaction the log keeps
	// every enqueue/lease/ack forever.
	for i := 0; i < 2000; i++ {
		j := mk("a")
		if i < 50 { // idempotency keys are retained state; keep them few here
			j.IdempotencyKey = fmt.Sprint("k", i)
		}
		enq(t, p, j)
		got := deq1(t, p, "w")
		if err := p.Ack(bg, got.ID, got.Attempt); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		enq(t, p, mk("keep"))
	}
	bad := deq1(t, p, "w")
	p.Fail(bg, bad.ID, bad.Attempt, Failure{NonRetryable: true, Err: "x"})

	before := p.Stats().LogBytes
	want := view(p, clk.Now())
	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	after := p.Stats().LogBytes
	if after >= before/10 {
		t.Errorf("compaction %d -> %d bytes; expected a large reduction", before, after)
	}
	// Writes after compaction land after the snapshot.
	post := enq(t, p, mk("post"))
	want[post] = view(p, clk.Now())[post]
	p.Close()

	p2, _ := openWAL(t, dir, clk)
	defer p2.Close()
	sameView(t, "after compaction + restart", want, view(p2, clk.Now()))
	dup := mk("a")
	dup.IdempotencyKey = "k49"
	if r, _ := p2.Enqueue(bg, dup); !r.Duplicate {
		t.Error("idempotency key lost in compaction")
	}
	if p2.Stats().Compactions != 0 {
		t.Error("compaction counter is per-process")
	}
}

func TestAutomaticCompactionRunsFromRunLoop(t *testing.T) {
	dir := t.TempDir()
	p, _ := openWAL(t, dir, clock.Real{}, func(c *Config) { c.CompactMinBytes = 20 << 10 })
	ctx, cancel := context.WithCancel(bg)
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	defer func() { cancel(); <-done; p.Close() }()
	for i := 0; i < 600; i++ {
		id := enq(t, p, mk("a"))
		j := deq1(t, p, "w")
		if err := p.Ack(bg, id, j.Attempt); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for p.Stats().Compactions == 0 {
		if time.Now().After(deadline) {
			t.Fatalf("no automatic compaction; log is %d bytes", p.Stats().LogBytes)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Property: for any sequence of operations and time jumps, recovering from
// the log yields exactly the state the live partition had. This is the
// invariant that makes the log trustworthy as the source of truth.
func TestReplayEquivalenceRandomOps(t *testing.T) {
	for seed := uint64(1); seed <= 25; seed++ {
		t.Run(fmt.Sprint("seed", seed), func(t *testing.T) {
			rng := rand.New(rand.NewPCG(seed, seed*7919))
			clk := clock.NewFake(t0)
			log := storage.NewMemLog()
			p, _ := newP(t, func(c *Config) { c.Log = log; c.Clock = clk; c.MaxDepth = 50 })

			type lease struct {
				id      string
				attempt int
			}
			var held []lease
			var allIDs []string
			for step := 0; step < 400; step++ {
				switch op := rng.IntN(10); {
				case op <= 2:
					j := mk("a")
					j.Priority = 1 + rng.IntN(10)
					if rng.IntN(4) == 0 {
						j.RunAt = clk.Now().Add(time.Duration(rng.IntN(60)) * time.Second)
					}
					if rng.IntN(5) == 0 {
						j.IdempotencyKey = fmt.Sprint("k", rng.IntN(10))
					}
					if r, err := p.Enqueue(bg, j); err == nil {
						allIDs = append(allIDs, r.ID)
					}
				case op <= 4:
					js, _ := p.Dequeue(bg, DequeueRequest{Worker: fmt.Sprint("w", rng.IntN(3)), Max: 1 + rng.IntN(3)})
					for _, j := range js {
						held = append(held, lease{j.ID, j.Attempt})
					}
				case op == 5 && len(held) > 0:
					l := held[rng.IntN(len(held))]
					p.Ack(bg, l.id, l.attempt)
				case op == 6 && len(held) > 0:
					l := held[rng.IntN(len(held))]
					p.Fail(bg, l.id, l.attempt, Failure{Err: "e", NonRetryable: rng.IntN(5) == 0})
				case op == 7 && len(held) > 0:
					l := held[rng.IntN(len(held))]
					p.Renew(bg, l.id, l.attempt, time.Duration(1+rng.IntN(20))*time.Second)
				case op == 8 && len(allIDs) > 0:
					id := allIDs[rng.IntN(len(allIDs))]
					if rng.IntN(2) == 0 {
						p.Cancel(bg, id)
					} else if rng.IntN(2) == 0 {
						p.ReplayDead(bg, id)
					} else {
						p.DeleteDead(bg, id)
					}
				default:
					clk.Advance(time.Duration(rng.IntN(40)) * time.Second)
				}
				if step%100 == 99 {
					if err := p.Compact(); err != nil {
						t.Fatal(err)
					}
				}
			}
			want := view(p, clk.Now())
			p2, err := Open(Config{Name: "re", Log: log, Clock: clk, Retry: testPolicy,
				DefaultVisibility: 10 * time.Second, MaxDepth: 50})
			if err != nil {
				t.Fatal(err)
			}
			sameView(t, "replay", want, view(p2, clk.Now()))
			if a, b := p.Stats(), p2.Stats(); a.Ready != b.Ready || a.Scheduled != b.Scheduled ||
				a.InFlight != b.InFlight || a.Dead != b.Dead {
				t.Fatalf("counts diverged: live %+v recovered %+v", a, b)
			}
		})
	}
}

// Cumulative counters for logged events must survive restart, failover (which
// is a restart on another node) and compaction. Otherwise the stats API
// reports totals that silently drop whatever happened before the last
// leadership change, which looks exactly like lost jobs.
func TestCumulativeCountersSurviveRecoveryAndCompaction(t *testing.T) {
	log := storage.NewMemLog()
	clk := clock.NewFake(t0)
	p, _ := newP(t, func(c *Config) { c.Log = log; c.Clock = clk })

	for i := 0; i < 20; i++ {
		j := mk("a")
		j.IdempotencyKey = fmt.Sprint("k", i)
		enq(t, p, j)
	}
	for i := 0; i < 12; i++ {
		got := deq1(t, p, "w")
		if i < 8 {
			if err := p.Ack(bg, got.ID, got.Attempt); err != nil {
				t.Fatal(err)
			}
		} else if i < 11 {
			p.Fail(bg, got.ID, got.Attempt, Failure{Err: "x"})
		} else {
			p.Fail(bg, got.ID, got.Attempt, Failure{Err: "x", NonRetryable: true})
		}
	}
	want := p.Stats()
	if want.Enqueued != 20 || want.Leased != 12 || want.Acked != 8 || want.Retried != 3 || want.DeadLettered != 1 {
		t.Fatalf("live counters: %+v", want)
	}
	same := func(label string, got Stats) {
		t.Helper()
		if got.Enqueued != want.Enqueued || got.Leased != want.Leased || got.Acked != want.Acked ||
			got.Retried != want.Retried || got.DeadLettered != want.DeadLettered {
			t.Fatalf("%s: enqueued/leased/acked/retried/dead = %d/%d/%d/%d/%d, want %d/%d/%d/%d/%d", label,
				got.Enqueued, got.Leased, got.Acked, got.Retried, got.DeadLettered,
				want.Enqueued, want.Leased, want.Acked, want.Retried, want.DeadLettered)
		}
	}
	reopen := func() *Partition {
		p2, err := Open(Config{Name: "re", Log: log, Clock: clk, Retry: testPolicy, DefaultVisibility: 10 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return p2
	}
	same("after recovery from the full log", reopen().Stats())

	if err := p.Compact(); err != nil {
		t.Fatal(err)
	}
	same("after compaction", p.Stats())
	same("after recovery from a compacted log", reopen().Stats())

	// New activity continues from the recovered totals instead of from zero.
	p3 := reopen()
	enq(t, p3, mk("more"))
	if got := p3.Stats().Enqueued; got != 21 {
		t.Fatalf("enqueued = %d after one more enqueue, want 21", got)
	}
}
