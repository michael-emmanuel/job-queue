package partition

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

// trackedLog remembers the durability wait of the most recent append.
//
// Durability is cumulative: when a later record is durable (fsynced, or
// committed by a quorum) every earlier one is too. So waiting on the latest
// append is a sound way to wait for "everything accepted so far", which is
// what an idempotent retry needs: it may only be acknowledged once the write
// it is a duplicate of can no longer be lost. Appends happen under the
// partition lock, so last is always the newest.
type trackedLog struct {
	storage.Log
	mu   sync.Mutex
	last storage.WaitFunc
}

func (t *trackedLog) Append(r storage.Record) (storage.WaitFunc, error) {
	w, err := t.Log.Append(r)
	if err == nil {
		t.mu.Lock()
		t.last = w
		t.mu.Unlock()
	}
	return w, err
}

func (t *trackedLog) latest() storage.WaitFunc {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}

// Partition owns a disjoint subset of a queue's jobs.
//
// Concurrency model: one mutex per partition guards all state. It is held
// only for in-memory work and for buffering a record into the WAL; it is
// never held across an fsync (callers wait for durability after unlocking),
// which is what allows group commit. There is no global lock: partitions do
// not share state, so throughput scales with partition count until the disk
// or CPU saturates. A mutex (not channels) is used because the state is a
// set of tightly coupled indexes that must change together atomically;
// channels would force an actor goroutine and a copy per operation. Channels
// are used only for signalling (wake, avail), where a closed channel is the
// natural broadcast that composes with select, contexts and timers.
type Partition struct {
	cfg Config
	log *trackedLog

	mu        sync.Mutex
	jobs      map[string]*job.Job
	dead      map[string]*job.DeadLetter
	idem      map[string]idemEntry
	idemOrder []idemRef
	idemHead  int
	ready     [numClasses]fifo
	credits   [numClasses]int
	timers    timerHeap
	counts    [int(job.StateComplete) + 1]int
	seq       uint64
	closed    bool
	avail     chan struct{} // closed to wake long-pollers; nil when nobody waits
	stats     counters

	wake            chan struct{} // cap 1; tells Run the earliest deadline changed
	compactMu       sync.Mutex    // serialises Compact
	lastCompactSize int64
}

type counters struct {
	enqueued, duplicates, rejected  uint64
	leased, acked, retried          uint64
	deadLettered, expired, compacts uint64
}

// Stats is a point-in-time snapshot for metrics and tests.
type Stats struct {
	Ready, Scheduled, InFlight, Dead int
	Enqueued, Duplicates, Rejected   uint64
	Leased, Acked, Retried           uint64
	DeadLettered, Expired            uint64
	Compactions                      uint64
	LogBytes                         int64
}

// Open recovers a partition from cfg.Log.
//
// Recovery replays every record to rebuild jobs, then rebuilds the ready
// queues and timer heap from them. Jobs that were in flight at crash time keep
// their original visibility deadline: a worker that is still alive may ack
// until it, and after it the job is redelivered. If the partition was down
// longer than the deadline, such jobs become available immediately.
func Open(cfg Config) (*Partition, error) {
	if err := cfg.setDefaults(); err != nil {
		return nil, err
	}
	p := &Partition{
		cfg:     cfg,
		log:     &trackedLog{Log: cfg.Log},
		jobs:    make(map[string]*job.Job),
		dead:    make(map[string]*job.DeadLetter),
		idem:    make(map[string]idemEntry),
		credits: cfg.Weights,
		wake:    make(chan struct{}, 1),
	}
	start := time.Now()
	if err := cfg.Log.Replay(p.applyRecord); err != nil {
		return nil, fmt.Errorf("partition %s: recovery: %w", cfg.Name, err)
	}
	p.mu.Lock()
	p.rebuildLocked()
	p.advanceLocked(p.cfg.Clock.Now())
	p.lastCompactSize = p.log.Size()
	st := p.statsLocked()
	p.mu.Unlock()
	cfg.Logger.Info("partition recovered",
		"ready", st.Ready, "scheduled", st.Scheduled, "in_flight", st.InFlight,
		"dead", st.Dead, "log_bytes", st.LogBytes, "took", time.Since(start))
	return p, nil
}

// rebuildLocked reconstructs the derived indexes (ready queues, timers) from
// canonical state. FIFO order is restored from Seq.
func (p *Partition) rebuildLocked() {
	for i := range p.ready {
		p.ready[i].reset()
	}
	p.timers = p.timers[:0]
	all := make([]*job.Job, 0, len(p.jobs))
	for _, j := range p.jobs {
		all = append(all, j)
	}
	sort.Slice(all, func(a, b int) bool { return all[a].Seq < all[b].Seq })
	for _, j := range all {
		switch j.State {
		case job.StateReady:
			p.ready[classOf(j.Priority)].push(j.ID)
		case job.StateScheduled:
			pushHeap(&p.timers, timerItem{at: j.VisibleAt, id: j.ID, attempt: j.Attempt, kind: timerVisible})
		case job.StateInFlight:
			pushHeap(&p.timers, timerItem{at: j.VisibleAt, id: j.ID, attempt: j.Attempt, kind: timerLease})
		}
	}
}

// Run drives time-based transitions (scheduled jobs, retry backoff,
// visibility timeouts, idempotency expiry, compaction) until ctx is done.
// It sleeps until the earliest known deadline instead of polling, so an idle
// partition costs nothing and a busy one wakes exactly when work is due.
// Exactly one Run goroutine should exist per partition; its lifetime is the
// caller's context.
func (p *Partition) Run(ctx context.Context) error {
	const maxIdle = time.Second // bounds how stale idempotency sweeping and compaction checks can get
	timer := time.NewTimer(maxIdle)
	defer timer.Stop()
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return ErrClosed
		}
		now := p.cfg.Clock.Now()
		p.advanceLocked(now)
		wait := maxIdle
		if len(p.timers) > 0 {
			if d := p.timers[0].at.Sub(now); d < wait {
				wait = max(d, 0)
			}
		}
		compact := p.cfg.CompactMinBytes > 0 && p.log.Size() >= max(p.cfg.CompactMinBytes, 2*p.lastCompactSize)
		p.mu.Unlock()

		if compact {
			if err := p.Compact(); err != nil {
				p.cfg.Logger.Error("compaction failed", "err", err)
			}
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		case <-p.wake:
		}
	}
}

// Compact rewrites the log as a snapshot of current state, discarding the
// history of acks, retries and leases. The lock is held only to copy state
// and rotate the segment; encoding and file I/O happen outside it.
func (p *Partition) Compact() error {
	p.compactMu.Lock()
	defer p.compactMu.Unlock()

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	state := p.captureLocked(p.cfg.Clock.Now())
	marker, err := p.log.Rotate()
	p.mu.Unlock()
	if err != nil {
		return err
	}
	before := p.log.Size()
	if err := p.log.WriteSnapshot(marker, state.records()); err != nil {
		return err
	}
	p.mu.Lock()
	p.lastCompactSize = p.log.Size()
	p.stats.compacts++
	p.mu.Unlock()
	p.cfg.Logger.Info("compacted", "bytes_before", before, "bytes_after", p.log.Size(),
		"jobs", len(state.jobs), "dead", len(state.dead))
	return nil
}

// Close stops the partition, wakes blocked consumers (they receive
// ErrClosed) and closes the underlying log, making buffered records durable.
func (p *Partition) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	p.notifyLocked()
	p.mu.Unlock()
	return p.log.Close()
}

// Stats returns a snapshot of counts and counters.
func (p *Partition) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.statsLocked()
}

func (p *Partition) statsLocked() Stats {
	return Stats{
		Ready:        p.counts[job.StateReady],
		Scheduled:    p.counts[job.StateScheduled],
		InFlight:     p.counts[job.StateInFlight],
		Dead:         p.counts[job.StateDead],
		Enqueued:     p.stats.enqueued,
		Duplicates:   p.stats.duplicates,
		Rejected:     p.stats.rejected,
		Leased:       p.stats.leased,
		Acked:        p.stats.acked,
		Retried:      p.stats.retried,
		DeadLettered: p.stats.deadLettered,
		Expired:      p.stats.expired,
		Compactions:  p.stats.compacts,
		LogBytes:     p.log.Size(),
	}
}

// liveLocked is the backlog the MaxDepth limit applies to.
func (p *Partition) liveLocked() int {
	return p.counts[job.StateReady] + p.counts[job.StateScheduled] + p.counts[job.StateInFlight]
}

// advanceLocked performs every time-based transition due at `now`:
// scheduled/backoff jobs become ready and lapsed leases are released. It is
// called at the start of every operation (so correctness never depends on
// the Run goroutine's timing) and by Run (so waiting consumers wake promptly).
func (p *Partition) advanceLocked(now time.Time) {
	promoted := false
	for len(p.timers) > 0 && !p.timers[0].at.After(now) {
		it := popHeap(&p.timers)
		j := p.jobs[it.id]
		if j == nil {
			continue
		}
		switch it.kind {
		case timerVisible:
			if j.State == job.StateScheduled && j.VisibleAt.Equal(it.at) {
				p.setState(j, job.StateReady)
				p.ready[classOf(j.Priority)].push(j.ID)
				promoted = true
			}
		case timerLease:
			if j.State == job.StateInFlight && j.Attempt == it.attempt && j.VisibleAt.Equal(it.at) {
				p.expireLocked(j, now)
				promoted = true
			}
		}
	}
	p.sweepIdemLocked(now)
	if promoted {
		p.notifyLocked()
	}
}

// expireLocked handles a lapsed visibility timeout. The job returns to Ready
// with its lease owner intact: until someone else leases it, the original
// worker's late ack is still accepted (no duplicate work if it was merely
// slow). Returning to Ready is derived state and is not logged; recovery
// recomputes it from the lease deadline. A job whose attempts are exhausted
// is a poison message (it keeps killing or stalling workers) and goes to
// the DLQ, which is logged because it creates new durable state.
func (p *Partition) expireLocked(j *job.Job, now time.Time) {
	p.stats.expired++
	if j.Attempt >= j.MaxAttempts {
		rec := deadRec{ID: j.ID, Attempt: j.Attempt, Reason: job.ReasonVisibilityExhausted,
			Err: "visibility timeout expired", Worker: j.LeaseOwner, At: now}
		if _, err := p.log.Append(mustEncode(recDead, rec)); err == nil {
			p.applyDead(rec)
			p.stats.deadLettered++
			p.cfg.Logger.Warn("job dead-lettered", "job_id", j.ID, "reason", rec.Reason, "attempt", rec.Attempt)
			return
		} else {
			// The log is failing. Fall through to Ready; the next expiry
			// retries the transition. Never leave a job with no timer.
			p.cfg.Logger.Error("dead-letter append failed", "job_id", j.ID, "err", err)
		}
	}
	j.LastError = "visibility timeout expired"
	p.setState(j, job.StateReady)
	p.ready[classOf(j.Priority)].push(j.ID)
	p.cfg.Logger.Info("lease expired", "job_id", j.ID, "attempt", j.Attempt, "worker", j.LeaseOwner)
}

// Get returns a copy of a live or dead-lettered job.
func (p *Partition) Get(id string) (job.Job, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if j := p.jobs[id]; j != nil {
		return *j, nil
	}
	if d := p.dead[id]; d != nil {
		return d.Job, nil
	}
	return job.Job{}, ErrNotFound
}

// Healthy reports whether the partition can still accept writes. A log that
// has failed (for example after an fsync error) is permanent until restart,
// and the broker uses this to fail readiness probes so traffic moves away.
func (p *Partition) Healthy() error {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return ErrClosed
	}
	if h, ok := p.log.Log.(interface{ Err() error }); ok {
		return h.Err()
	}
	return nil
}
