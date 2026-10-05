package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/observability"
	"github.com/michael-emmanuel/jobqueue/pkg/client"
)

var bg = context.Background()

// aligned is a multiple of 60, so it is the first second of a one-minute slot.
const aligned = 1_000_020

// fakeQueue models the queue's idempotency behaviour.
type fakeQueue struct {
	mu   sync.Mutex
	keys map[string]string
	jobs []string
	fail error
}

func (f *fakeQueue) Submit(_ context.Context, r client.SubmitRequest) (client.SubmitResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail != nil {
		return client.SubmitResult{}, f.fail
	}
	if f.keys == nil {
		f.keys = map[string]string{}
	}
	if id, ok := f.keys[r.IdempotencyKey]; ok {
		return client.SubmitResult{JobID: id, Duplicate: true}, nil
	}
	id := "job-" + r.IdempotencyKey
	f.keys[r.IdempotencyKey] = id
	f.jobs = append(f.jobs, r.IdempotencyKey)
	return client.SubmitResult{JobID: id}, nil
}

func cfg(every time.Duration) Config {
	return Config{Schedules: []Schedule{{Name: "report", Every: Duration(every), Queue: "reports", Type: "gen",
		Payload: json.RawMessage(`{"x":1}`)}}}
}

func TestOneSubmissionPerSlot(t *testing.T) {
	clk := clock.NewFake(time.Unix(aligned, 0))
	q := &fakeQueue{}
	s, _ := New(q, cfg(time.Minute), clk, observability.Discard())
	if s.Tick(bg) != 1 {
		t.Fatal("first tick must submit")
	}
	for i := 0; i < 10; i++ { // many ticks inside the same minute
		clk.Advance(5 * time.Second)
		if i < 9 && s.Tick(bg) != 0 {
			t.Fatalf("tick %d re-submitted inside the same slot", i)
		}
	}
	clk.Advance(30 * time.Second)
	if s.Tick(bg) != 1 {
		t.Fatal("next slot must submit")
	}
	if len(q.jobs) != 2 {
		t.Fatalf("jobs: %v", q.jobs)
	}
}

// Two replicas, one queue: each slot yields exactly one job.
func TestReplicasDoNotDoubleSubmit(t *testing.T) {
	clk := clock.NewFake(time.Unix(aligned, 0))
	q := &fakeQueue{}
	a, _ := New(q, cfg(time.Minute), clk, observability.Discard())
	b, _ := New(q, cfg(time.Minute), clk, observability.Discard())
	for i := 0; i < 20; i++ {
		a.Tick(bg)
		b.Tick(bg)
		clk.Advance(20 * time.Second)
	}
	// 20 ticks x 20s = 400s = 6-7 distinct minutes.
	slots := map[int64]bool{}
	for t := 0; t < 20; t++ {
		slots[Slot(time.Unix(aligned, 0).Add(time.Duration(t)*20*time.Second), time.Minute)] = true
	}
	if len(q.jobs) != len(slots) {
		t.Fatalf("%d jobs for %d slots: replicas double-submitted (%v)", len(q.jobs), len(slots), q.jobs)
	}
	if a.Deduplicated+b.Deduplicated == 0 {
		t.Fatal("the second replica should have hit the idempotency key at least once")
	}
}

func TestSurvivingReplicaCoversForADeadOne(t *testing.T) {
	clk := clock.NewFake(time.Unix(2_000_000, 0))
	q := &fakeQueue{}
	a, _ := New(q, cfg(time.Minute), clk, observability.Discard())
	b, _ := New(q, cfg(time.Minute), clk, observability.Discard())
	a.Tick(bg)
	b.Tick(bg)
	clk.Advance(time.Minute) // replica a has died; only b ticks from now on
	if b.Tick(bg) != 1 {
		t.Fatal("the survivor must submit the next slot")
	}
	if len(q.jobs) != 2 {
		t.Fatalf("%v", q.jobs)
	}
}

func TestFailedSubmitRetriesWhileTheSlotIsCurrentThenSkips(t *testing.T) {
	clk := clock.NewFake(time.Unix(3_000_000, 0))
	q := &fakeQueue{fail: errors.New("api down")}
	s, _ := New(q, cfg(time.Minute), clk, observability.Discard())
	s.Tick(bg)
	clk.Advance(10 * time.Second)
	s.Tick(bg)
	if s.Errors != 2 || len(q.jobs) != 0 {
		t.Fatalf("errors=%d jobs=%v", s.Errors, q.jobs)
	}
	q.fail = nil
	clk.Advance(10 * time.Second) // API is back, still the same slot
	if s.Tick(bg) != 1 {
		t.Fatal("must submit once the API recovers within the slot")
	}
	// Outage across several slots: no back-fill.
	q.fail = errors.New("down")
	clk.Advance(5 * time.Minute)
	s.Tick(bg)
	q.fail = nil
	before := len(q.jobs)
	clk.Advance(time.Second)
	s.Tick(bg)
	if len(q.jobs)-before != 1 {
		t.Fatalf("after a long outage exactly the current slot should be submitted, got %d", len(q.jobs)-before)
	}
}

func TestSlotKeysAreStableAcrossReplicasAndTimeZones(t *testing.T) {
	const slotStart = 1_700_000_040 // 28333334 * 60
	a := time.Unix(slotStart+5, 0).In(time.UTC)
	b := time.Unix(slotStart+59, 0).In(time.FixedZone("x", 9*3600))
	if Key("r", Slot(a, time.Minute)) != Key("r", Slot(b, time.Minute)) {
		t.Fatal("same instant range must map to the same key regardless of zone")
	}
	if Key("r", Slot(a, time.Minute)) == Key("r", Slot(a.Add(time.Minute), time.Minute)) {
		t.Fatal("adjacent slots collided")
	}
}

func TestConfigValidation(t *testing.T) {
	good := Schedule{Name: "a", Every: Duration(time.Minute), Queue: "q", Type: "t"}
	bad := map[string]Config{
		"empty":           {},
		"duplicate names": {Schedules: []Schedule{good, good}},
		"too frequent":    {Schedules: []Schedule{{Name: "a", Every: Duration(time.Millisecond), Queue: "q", Type: "t"}}},
		"outlives dedupe": {Schedules: []Schedule{{Name: "a", Every: Duration(48 * time.Hour), Queue: "q", Type: "t"}}},
		"missing queue":   {Schedules: []Schedule{{Name: "a", Every: Duration(time.Minute), Type: "t"}}},
	}
	for name, c := range bad {
		if err := c.Validate(); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
	if err := (Config{Schedules: []Schedule{good}}).Validate(); err != nil {
		t.Fatal(err)
	}
	var c Config
	if err := json.Unmarshal([]byte(`{"schedules":[{"name":"a","every":"90s","queue":"q","type":"t"}]}`), &c); err != nil || time.Duration(c.Schedules[0].Every) != 90*time.Second {
		t.Fatalf("%v %v", c, err)
	}
	if err := json.Unmarshal([]byte(`{"schedules":[{"every":90}]}`), &c); err == nil || !strings.Contains(err.Error(), "string") {
		t.Fatalf("numeric duration accepted: %v", err)
	}
}
