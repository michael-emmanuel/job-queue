// Package scheduler submits recurring jobs ("every minute, enqueue a report").
//
// One-off delayed jobs need no scheduler process: they carry run_at and wait
// in the partition's timer heap. This package is for jobs that recur.
//
// High availability without coordination: each recurrence has a SLOT, the
// integer part of unix_time / period, and the job's idempotency key is
// "<schedule>:<slot>". Any number of scheduler replicas may run; all of them
// submit the same key for the same slot and the queue keeps one. There is no
// leader election to get wrong, and a replica dying loses nothing because the
// survivors submit the same slot. This relies on the queue's idempotency
// window (default 24h) being longer than a slot and than any clock skew
// between replicas.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/pkg/client"
)

// Schedule is one recurring job.
type Schedule struct {
	Name     string          `json:"name"`
	Every    Duration        `json:"every"`
	Queue    string          `json:"queue"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	Priority int             `json:"priority,omitempty"`
}

// Duration is a time.Duration that unmarshals from a string such as "5m".
type Duration time.Duration

// UnmarshalJSON implements json.Unmarshaler.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New(`"every" must be a string such as "5m"`)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d = Duration(v)
	return nil
}

// Config is the scheduler's configuration file.
type Config struct {
	Schedules []Schedule `json:"schedules"`
}

// Validate checks a configuration.
func (c Config) Validate() error {
	if len(c.Schedules) == 0 {
		return errors.New("no schedules configured")
	}
	seen := map[string]bool{}
	for i, s := range c.Schedules {
		switch {
		case s.Name == "" || seen[s.Name]:
			return fmt.Errorf("schedule %d: name must be present and unique", i)
		case time.Duration(s.Every) < time.Second:
			return fmt.Errorf("schedule %q: every must be at least 1s", s.Name)
		case time.Duration(s.Every) > 24*time.Hour:
			return fmt.Errorf("schedule %q: every above 24h would outlive the default idempotency window, so replicas could double-submit", s.Name)
		case s.Queue == "" || s.Type == "":
			return fmt.Errorf("schedule %q: queue and type are required", s.Name)
		}
		seen[s.Name] = true
	}
	return nil
}

// Submitter is the part of the client the scheduler needs.
type Submitter interface {
	Submit(ctx context.Context, r client.SubmitRequest) (client.SubmitResult, error)
}

// Scheduler runs a set of schedules.
type Scheduler struct {
	sub Submitter
	cfg Config
	clk clock.Clock
	log *slog.Logger

	mu       sync.Mutex
	lastSlot map[string]int64
	// Counters, for metrics and tests.
	Submitted, Deduplicated, Errors uint64
}

// New returns a Scheduler. clk may be nil for the wall clock.
func New(sub Submitter, cfg Config, clk clock.Clock, log *slog.Logger) (*Scheduler, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	return &Scheduler{sub: sub, cfg: cfg, clk: clk, log: log, lastSlot: map[string]int64{}}, nil
}

// Slot returns the recurrence number containing t.
func Slot(t time.Time, every time.Duration) int64 { return t.Unix() / int64(every.Seconds()) }

// Key is the idempotency key for a schedule's slot.
func Key(name string, slot int64) string { return fmt.Sprintf("%s:%d", name, slot) }

// Tick submits every schedule whose current slot has not been submitted by
// this process yet. A failed submission is retried on the next tick for as
// long as the slot is still current; a slot that passes while the API was
// unreachable is skipped, not back-filled, because a stale "every minute"
// job running late is usually worse than missing one.
func (s *Scheduler) Tick(ctx context.Context) int {
	now := s.clk.Now()
	n := 0
	for _, sc := range s.cfg.Schedules {
		slot := Slot(now, time.Duration(sc.Every))
		s.mu.Lock()
		done := s.lastSlot[sc.Name] == slot
		s.mu.Unlock()
		if done {
			continue
		}
		res, err := s.sub.Submit(ctx, client.SubmitRequest{Queue: sc.Queue, Type: sc.Type, Payload: sc.Payload,
			Priority: sc.Priority, IdempotencyKey: Key(sc.Name, slot)})
		s.mu.Lock()
		switch {
		case err != nil:
			s.Errors++
			s.log.Warn("schedule submit failed; will retry while the slot is current", "schedule", sc.Name, "slot", slot, "err", err)
		case res.Duplicate:
			s.lastSlot[sc.Name] = slot
			s.Deduplicated++
			s.log.Debug("slot already submitted by another replica", "schedule", sc.Name, "slot", slot, "job_id", res.JobID)
		default:
			s.lastSlot[sc.Name] = slot
			s.Submitted++
			n++
			s.log.Info("submitted", "schedule", sc.Name, "slot", slot, "job_id", res.JobID)
		}
		s.mu.Unlock()
	}
	return n
}

// Run ticks until ctx is done.
func (s *Scheduler) Run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	s.Tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.Tick(ctx)
		}
	}
}
