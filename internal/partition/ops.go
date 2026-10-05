package partition

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

// Every mutating operation follows the same shape:
//
//	lock; advance time; validate; append record to the WAL; apply it; index;
//	unlock; then wait for durability.
//
// Appending before applying is the write-ahead rule: state never gets ahead
// of the log. Waiting after unlocking is what allows group commit. The cost
// of that split is a small window where an operation's effect is visible to
// other callers before it is durable; each operation documents whether that
// matters.

// EnqueueResult reports the outcome of Enqueue.
type EnqueueResult struct {
	ID string
	// Duplicate is true when an existing job with the same idempotency key
	// was found and no new job was created.
	Duplicate bool
}

func (p *Partition) validate(j *job.Job) error {
	switch {
	case j.Queue == "":
		return fmt.Errorf("%w: queue is required", ErrInvalidJob)
	case j.Type == "":
		return fmt.Errorf("%w: type is required", ErrInvalidJob)
	case j.MaxAttempts < 0:
		return fmt.Errorf("%w: max_attempts must be >= 0", ErrInvalidJob)
	case len(j.Payload) > p.cfg.MaxPayloadBytes:
		return fmt.Errorf("%w: payload is %d bytes, limit %d", ErrInvalidJob, len(j.Payload), p.cfg.MaxPayloadBytes)
	}
	if j.Priority == 0 {
		j.Priority = job.DefaultPriority
	}
	if j.Priority < job.MinPriority || j.Priority > job.MaxPriority {
		return fmt.Errorf("%w: priority must be in [%d,%d]", ErrInvalidJob, job.MinPriority, job.MaxPriority)
	}
	if len(j.Payload) == 0 {
		j.Payload = json.RawMessage("null")
	} else if !json.Valid(j.Payload) {
		return fmt.Errorf("%w: payload is not valid JSON", ErrInvalidJob)
	}
	if j.Tenant == "" {
		j.Tenant = "default"
	}
	return nil
}

// Enqueue adds a job. If the job carries an idempotency key that is still
// remembered, no job is created and the original job's ID is returned.
//
// Return semantics: a nil error means the job is durable per the log's sync
// policy. A non-nil error from the durability wait (context cancelled,
// fsync failure) does NOT mean the job was not created: it is already in
// memory and in the log's order. Callers must retry with the same
// idempotency key, which is exactly why the key exists.
func (p *Partition) Enqueue(ctx context.Context, in job.Job) (EnqueueResult, error) {
	if err := p.validate(&in); err != nil {
		return EnqueueResult{}, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return EnqueueResult{}, ErrClosed
	}
	now := p.cfg.Clock.Now()
	p.advanceLocked(now)

	if in.IdempotencyKey != "" {
		if e, ok := p.idem[idemScope(&in)]; ok && now.Before(e.expires) {
			p.stats.duplicates++
			wait := p.log.latest()
			p.mu.Unlock()
			// Acknowledge only once the original (and everything after it)
			// is durable; the key lives in memory the moment the original is
			// applied, which is before it is safe.
			if wait != nil {
				if err := wait(ctx); err != nil {
					return EnqueueResult{ID: e.jobID, Duplicate: true}, err
				}
			}
			return EnqueueResult{ID: e.jobID, Duplicate: true}, nil
		}
	}
	if p.liveLocked() >= p.cfg.MaxDepth {
		p.stats.rejected++
		p.mu.Unlock()
		return EnqueueResult{}, ErrQueueFull
	}

	if in.ID == "" {
		in.ID = job.NewID()
	}
	if in.MaxAttempts == 0 {
		in.MaxAttempts = p.cfg.Retry.MaxAttempts
	}
	in.CreatedAt = now
	in.Seq = p.seq + 1
	in.Attempt = 0
	in.LeaseOwner = ""
	in.LastError = ""
	if in.RunAt.After(now) {
		in.State, in.VisibleAt = job.StateScheduled, in.RunAt
	} else {
		in.State, in.VisibleAt = job.StateReady, now
	}

	wait, err := p.log.Append(mustEncode(recEnqueue, in))
	if err != nil {
		p.mu.Unlock()
		return EnqueueResult{}, err
	}
	jp := p.putJob(in)
	if jp.State == job.StateScheduled {
		p.pushTimerLocked(timerItem{at: jp.VisibleAt, id: jp.ID, kind: timerVisible})
	} else {
		p.pushReadyLocked(jp)
	}
	p.stats.enqueued++
	p.mu.Unlock()

	if err := wait(ctx); err != nil {
		return EnqueueResult{ID: in.ID}, err
	}
	return EnqueueResult{ID: in.ID}, nil
}

// DequeueRequest describes a pull by a worker.
type DequeueRequest struct {
	Worker     string        // required; recorded as the lease owner
	Max        int           // maximum jobs to return (default 1, capped at 100)
	Visibility time.Duration // lease length (default Config.DefaultVisibility, capped at MaxVisibility)
	Wait       time.Duration // long-poll: how long to block when empty (0 = return immediately)
}

// Dequeue leases up to req.Max ready jobs to a worker. Each returned job has
// Attempt set to its delivery number and VisibleAt set to its visibility
// deadline; the worker must Ack or Fail it (quoting Attempt) before then,
// or Renew the lease, or the job becomes available again.
//
// Lease records ARE made durable before jobs are returned. Skipping that
// wait looks safe (a lost lease only causes a redelivery) but breaks
// fencing: after a crash the job would revert to its old attempt number and
// the next delivery would reuse a token that a surviving pre-crash worker
// still holds, letting that zombie's ack pass the fence. Group commit keeps
// the cost to one shared fsync per batch of concurrent dequeues. Under
// SyncPeriodic/SyncNone the log makes no durability promise, and neither
// does this guarantee.
//
// When no job is ready and req.Wait > 0, Dequeue blocks until a job becomes
// ready, Wait elapses (returns nil, nil), ctx is done, or the partition
// closes (ErrClosed). This is long polling: it cuts empty-poll traffic and
// the latency between enqueue and delivery without server push.
func (p *Partition) Dequeue(ctx context.Context, req DequeueRequest) ([]job.Job, error) {
	if req.Worker == "" {
		return nil, fmt.Errorf("%w: worker is required", ErrInvalidJob)
	}
	req.Max = min(max(req.Max, 1), 100)
	if req.Visibility <= 0 {
		req.Visibility = p.cfg.DefaultVisibility
	}
	req.Visibility = min(req.Visibility, p.cfg.MaxVisibility)

	var timeout <-chan time.Time
	if req.Wait > 0 {
		t := time.NewTimer(req.Wait)
		defer t.Stop()
		timeout = t.C
	}
	for {
		jobs, avail, wait, err := p.tryDequeue(req)
		if err == nil && wait != nil {
			// Outside the partition lock, so concurrent dequeues share an fsync.
			// On failure the leases lapse by themselves; no job is lost.
			err = wait(ctx)
		}
		if err != nil {
			return nil, err
		}
		if len(jobs) > 0 || timeout == nil {
			return jobs, nil
		}
		select {
		case <-avail:
		case <-timeout:
			return nil, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (p *Partition) tryDequeue(req DequeueRequest) ([]job.Job, <-chan struct{}, storage.WaitFunc, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, nil, nil, ErrClosed
	}
	now := p.cfg.Clock.Now()
	p.advanceLocked(now)

	var (
		out  []job.Job
		last storage.WaitFunc // waiting on the last record covers all earlier ones
	)
	for len(out) < req.Max {
		j := p.pickLocked()
		if j == nil {
			break
		}
		rec := leaseRec{ID: j.ID, Attempt: j.Attempt + 1, Worker: req.Worker, Deadline: now.Add(req.Visibility)}
		wait, err := p.log.Append(mustEncode(recLease, rec))
		if err != nil {
			p.ready[classOf(j.Priority)].push(j.ID) // not leased; put it back
			if len(out) == 0 {
				return nil, nil, nil, err
			}
			break
		}
		last = wait
		p.applyLease(rec)
		p.pushTimerLocked(timerItem{at: rec.Deadline, id: j.ID, attempt: rec.Attempt, kind: timerLease})
		p.stats.leased++
		out = append(out, *j)
	}
	if len(out) > 0 {
		return out, nil, last, nil
	}
	return nil, p.availChLocked(), nil, nil
}

// holderLocked validates that (id, attempt) identifies the current lease.
//
// The attempt number is a fencing token. If the lease lapsed and the job was
// redelivered, Attempt has moved on and the old worker's ack is rejected, so
// a stale worker can never complete or fail a job that now belongs to
// someone else. If the lease lapsed but nobody has re-leased the job yet
// (state Ready with the lease owner still set), the late ack is accepted:
// the work is done, and discarding it would only cause a duplicate.
func (p *Partition) holderLocked(id string, attempt int) (*job.Job, error) {
	j := p.jobs[id]
	if j == nil {
		return nil, ErrNotFound
	}
	if attempt < 1 || j.Attempt != attempt || j.LeaseOwner == "" {
		return nil, ErrStaleLease
	}
	if j.State != job.StateInFlight && j.State != job.StateReady {
		return nil, ErrStaleLease
	}
	return j, nil
}

// Ack marks the job complete and removes it. A nil return means the removal
// is durable, so the job will not be redelivered after a crash.
func (p *Partition) Ack(ctx context.Context, id string, attempt int) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	now := p.cfg.Clock.Now()
	p.advanceLocked(now)
	if _, err := p.holderLocked(id, attempt); err != nil {
		p.mu.Unlock()
		return err
	}
	rec := ackRec{ID: id, Attempt: attempt, At: now}
	wait, err := p.log.Append(mustEncode(recAck, rec))
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.applyAck(rec)
	p.stats.acked++
	p.mu.Unlock()
	return wait(ctx)
}

// Failure describes a worker-reported failure.
type Failure struct {
	Err string
	// NonRetryable sends the job straight to the DLQ (e.g. malformed
	// payload, validation error) instead of burning retries on something that
	// can never succeed.
	NonRetryable bool
}

// Outcome is what Fail decided to do with the job.
type Outcome string

const (
	OutcomeRetry Outcome = "retry"
	OutcomeDead  Outcome = "dead"
)

// Fail reports that processing failed. The job is retried after exponential
// backoff with jitter, or dead-lettered if the failure is non-retryable or
// attempts are exhausted.
func (p *Partition) Fail(ctx context.Context, id string, attempt int, f Failure) (Outcome, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return "", ErrClosed
	}
	now := p.cfg.Clock.Now()
	p.advanceLocked(now)
	j, err := p.holderLocked(id, attempt)
	if err != nil {
		p.mu.Unlock()
		return "", err
	}

	var (
		wait    func(context.Context) error
		outcome Outcome
	)
	if f.NonRetryable || j.Attempt >= j.MaxAttempts {
		reason := job.ReasonMaxAttempts
		if f.NonRetryable {
			reason = job.ReasonNonRetryable
		}
		rec := deadRec{ID: id, Attempt: attempt, Reason: reason, Err: f.Err, Worker: j.LeaseOwner, At: now}
		w, err := p.log.Append(mustEncode(recDead, rec))
		if err != nil {
			p.mu.Unlock()
			return "", err
		}
		wait, outcome = w, OutcomeDead
		p.applyDead(rec)
		p.stats.deadLettered++
		p.cfg.Logger.Warn("job dead-lettered", "job_id", id, "reason", reason, "attempt", attempt)
	} else {
		delay := p.cfg.Retry.Delay(attempt, p.cfg.Rand)
		rec := retryRec{ID: id, Attempt: attempt, RunAt: now.Add(delay), Err: f.Err, At: now}
		w, err := p.log.Append(mustEncode(recRetry, rec))
		if err != nil {
			p.mu.Unlock()
			return "", err
		}
		wait, outcome = w, OutcomeRetry
		jp := p.applyRetry(rec)
		p.pushTimerLocked(timerItem{at: rec.RunAt, id: id, attempt: jp.Attempt, kind: timerVisible})
		p.stats.retried++
	}
	p.mu.Unlock()
	return outcome, wait(ctx)
}

// Renew extends the visibility deadline of an in-flight job to now+extend
// (capped at MaxVisibility) and returns the new deadline. This is the
// heartbeat that lets long jobs run with a short default timeout: crash
// detection stays fast while slow-but-alive jobs are not redelivered.
//
// Renew fails with ErrStaleLease once the lease has lapsed, even if nobody
// has re-leased the job yet, and with ErrNotFound if the job was cancelled;
// both tell the worker to stop. The record is not waited on: losing a renewal
// in a crash only shortens the lease, which causes at worst a redelivery.
func (p *Partition) Renew(ctx context.Context, id string, attempt int, extend time.Duration) (time.Time, error) {
	if extend <= 0 {
		extend = p.cfg.DefaultVisibility
	}
	extend = min(extend, p.cfg.MaxVisibility)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return time.Time{}, ErrClosed
	}
	now := p.cfg.Clock.Now()
	p.advanceLocked(now)
	j, err := p.holderLocked(id, attempt)
	if err != nil {
		return time.Time{}, err
	}
	if j.State != job.StateInFlight {
		return time.Time{}, ErrStaleLease
	}
	rec := renewRec{ID: id, Attempt: attempt, Deadline: now.Add(extend)}
	if _, err := p.log.Append(mustEncode(recRenew, rec)); err != nil {
		return time.Time{}, err
	}
	p.applyRenew(rec)
	p.pushTimerLocked(timerItem{at: rec.Deadline, id: id, attempt: attempt, kind: timerLease})
	return rec.Deadline, nil
}

// Cancel removes a live (ready, scheduled or in-flight) job. Cancelling an
// in-flight job is cooperative: the worker finds out when its next Renew or
// Ack returns ErrNotFound; this engine cannot interrupt remote work.
func (p *Partition) Cancel(ctx context.Context, id string) error {
	return p.remove(ctx, id, false)
}

// DeleteDead permanently removes a DLQ entry.
func (p *Partition) DeleteDead(ctx context.Context, id string) error {
	return p.remove(ctx, id, true)
}

func (p *Partition) remove(ctx context.Context, id string, dead bool) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	now := p.cfg.Clock.Now()
	p.advanceLocked(now)
	if dead && p.dead[id] == nil || !dead && p.jobs[id] == nil {
		p.mu.Unlock()
		return ErrNotFound
	}
	wait, err := p.log.Append(mustEncode(recDelete, deleteRec{ID: id, At: now}))
	if err != nil {
		p.mu.Unlock()
		return err
	}
	p.applyDelete(deleteRec{ID: id, At: now})
	p.mu.Unlock()
	return wait(ctx)
}

// DeadLetters lists up to limit DLQ entries, oldest first.
func (p *Partition) DeadLetters(limit int) []job.DeadLetter {
	p.mu.Lock()
	out := make([]job.DeadLetter, 0, len(p.dead))
	for _, d := range p.dead {
		out = append(out, *d)
	}
	p.mu.Unlock()
	sort.Slice(out, func(a, b int) bool {
		if !out[a].DeadAt.Equal(out[b].DeadAt) {
			return out[a].DeadAt.Before(out[b].DeadAt)
		}
		return out[a].OriginalJobID < out[b].OriginalJobID
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out
}

// ReplayDead moves a DLQ entry back to the ready state with its attempt
// counter reset. Replaying a job whose bug is not fixed just sends it back
// to the DLQ after MaxAttempts; that is by design.
func (p *Partition) ReplayDead(ctx context.Context, id string) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ErrClosed
	}
	now := p.cfg.Clock.Now()
	p.advanceLocked(now)
	if p.dead[id] == nil {
		p.mu.Unlock()
		return ErrNotFound
	}
	if p.liveLocked() >= p.cfg.MaxDepth {
		p.stats.rejected++
		p.mu.Unlock()
		return ErrQueueFull
	}
	rec := replayRec{ID: id, Seq: p.seq + 1, At: now}
	wait, err := p.log.Append(mustEncode(recReplay, rec))
	if err != nil {
		p.mu.Unlock()
		return err
	}
	jp := p.applyReplay(rec)
	p.pushReadyLocked(jp)
	p.mu.Unlock()
	return wait(ctx)
}
