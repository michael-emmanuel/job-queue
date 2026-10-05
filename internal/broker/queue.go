package broker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/ratelimit"
	"github.com/michael-emmanuel/jobqueue/internal/routing"
)

// signal wakes consumers waiting on any partition of a queue. Waiters grab
// the channel BEFORE scanning partitions and wait on it only if the scan
// found nothing; because fire closes the channel they hold, a job that
// arrives between the scan and the wait cannot be missed.
type signal struct {
	mu sync.Mutex
	ch chan struct{}
}

func (s *signal) wait() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ch == nil {
		s.ch = make(chan struct{})
	}
	return s.ch
}

func (s *signal) fire() {
	s.mu.Lock()
	if s.ch != nil {
		close(s.ch)
		s.ch = nil
	}
	s.mu.Unlock()
}

// Queue is a named set of partitions plus its limits.
type Queue struct {
	name    string
	cfg     QueueConfig
	created time.Time
	parts   []*partition.Partition
	sig     signal
	rr      atomic.Uint32 // spreads keyless enqueues and consumer scans
	enqLim  *ratelimit.Bucket
	conLim  *ratelimit.Bucket

	cancel func()
	wg     sync.WaitGroup
}

// Name returns the queue name.
func (q *Queue) Name() string { return q.name }

// Config returns the effective configuration.
func (q *Queue) Config() QueueConfig { return q.cfg }

// NumPartitions returns the partition count.
func (q *Queue) NumPartitions() int { return len(q.parts) }

func (q *Queue) close() error {
	q.cancel()
	q.wg.Wait()
	var first error
	for _, p := range q.parts {
		if err := p.Close(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// EnqueueResult describes an accepted submission.
type EnqueueResult struct {
	ID        string
	Partition int
	Duplicate bool
}

// Enqueue routes a job to a partition and appends it.
//
// Routing key: the explicit partition key if given (jobs sharing a key land
// on one partition and keep FIFO order relative to each other), else the
// idempotency key (so that a client retry reaches the partition that holds
// the dedupe record), else round-robin for an even spread.
func (q *Queue) Enqueue(ctx context.Context, j job.Job) (EnqueueResult, error) {
	if ok, wait := q.enqLim.Take(1); !ok {
		return EnqueueResult{}, &RateLimitedError{Scope: "queue_enqueue", RetryAfter: wait}
	}
	var part int
	switch {
	case j.PartitionKey != "":
		part = routing.Partition(q.name, j.PartitionKey, len(q.parts))
	case j.IdempotencyKey != "":
		part = routing.Partition(q.name, j.IdempotencyKey, len(q.parts))
	default:
		part = int(q.rr.Add(1)) % len(q.parts)
	}
	j.Queue = q.name
	j.ID = fmt.Sprintf("%s.%d.%s", q.name, part, job.NewID())
	r, err := q.parts[part].Enqueue(ctx, j)
	if err != nil {
		q.enqLim.Refund(1) // a rejected enqueue must not consume the queue's rate budget
		return EnqueueResult{ID: r.ID, Partition: part}, err
	}
	if r.Duplicate {
		q.enqLim.Refund(1)
	}
	return EnqueueResult{ID: r.ID, Partition: part, Duplicate: r.Duplicate}, nil
}

// PullRequest is a worker's request for jobs.
type PullRequest struct {
	Worker     string
	Max        int
	Visibility time.Duration
	Wait       time.Duration
}

// Pull leases up to req.Max jobs from any partition, starting at a rotating
// partition so no partition is systematically favoured. If nothing is ready
// and req.Wait > 0 it blocks until work arrives on any partition, the wait
// elapses, or ctx is done.
//
// The consume rate limit is applied by taking tokens for the batch up front
// and refunding the ones that went unused.
func (q *Queue) Pull(ctx context.Context, req PullRequest) ([]job.Job, error) {
	n := min(max(req.Max, 1), 100)
	if q.conLim != nil {
		got := q.conLim.TakeUpTo(n)
		if got == 0 {
			_, wait := q.conLim.Take(1)
			return nil, &RateLimitedError{Scope: "queue_consume", RetryAfter: wait}
		}
		n = got
	}
	var timeout <-chan time.Time
	if req.Wait > 0 {
		t := time.NewTimer(req.Wait)
		defer t.Stop()
		timeout = t.C
	}
	for {
		ch := q.sig.wait()
		jobs, err := q.scan(ctx, req, n)
		if err != nil || len(jobs) > 0 || timeout == nil {
			if len(jobs) < n {
				q.conLim.Refund(float64(n - len(jobs)))
			}
			return jobs, err
		}
		select {
		case <-ch:
		case <-timeout:
			q.conLim.Refund(float64(n))
			return nil, nil
		case <-ctx.Done():
			q.conLim.Refund(float64(n))
			return nil, ctx.Err()
		}
	}
}

func (q *Queue) scan(ctx context.Context, req PullRequest, n int) ([]job.Job, error) {
	start := int(q.rr.Add(1))
	var (
		out      []job.Job
		firstErr error
	)
	for i := 0; i < len(q.parts) && len(out) < n; i++ {
		p := q.parts[(start+i)%len(q.parts)]
		js, err := p.Dequeue(ctx, partition.DequeueRequest{
			Worker: req.Worker, Max: n - len(out), Visibility: req.Visibility,
		})
		if err != nil {
			// One failed partition must not hide work on healthy ones.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		out = append(out, js...)
	}
	if len(out) > 0 {
		return out, nil
	}
	return nil, firstErr
}

// DeadLetters lists DLQ entries across partitions, oldest first.
func (q *Queue) DeadLetters(limit int) []job.DeadLetter {
	var all []job.DeadLetter
	for _, p := range q.parts {
		all = append(all, p.DeadLetters(limit)...)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].DeadAt.Equal(all[j].DeadAt) {
			return all[i].DeadAt.Before(all[j].DeadAt)
		}
		return all[i].OriginalJobID < all[j].OriginalJobID
	})
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all
}

// QueueStats aggregates partition statistics.
type QueueStats struct {
	Name       string
	Config     QueueConfig
	Partitions []partition.Stats
	Total      partition.Stats
}

// Stats returns a point-in-time snapshot.
func (q *Queue) Stats() QueueStats {
	s := QueueStats{Name: q.name, Config: q.cfg}
	for _, p := range q.parts {
		s.Partitions = append(s.Partitions, p.Stats())
	}
	s.Total = SumStats(s.Partitions)
	return s
}

// Ref identifies where a job lives.
type Ref struct {
	Queue     string
	Partition int
}

// parseID splits "<queue>.<partition>.<id>".
func parseID(id string) (Ref, error) {
	parts := strings.SplitN(id, ".", 3)
	if len(parts) != 3 || !ValidName(parts[0]) || parts[2] == "" {
		return Ref{}, ErrInvalidID
	}
	p, err := strconv.Atoi(parts[1])
	if err != nil || p < 0 {
		return Ref{}, ErrInvalidID
	}
	return Ref{Queue: parts[0], Partition: p}, nil
}

// ParseID splits a job ID "<queue>.<partition>.<id>" into its route.
func ParseID(id string) (queue string, part int, err error) {
	ref, err := parseID(id)
	return ref.Queue, ref.Partition, err
}

// SumStats adds up per-partition statistics.
func SumStats(parts []partition.Stats) partition.Stats {
	var t partition.Stats
	for _, ps := range parts {
		t.Ready += ps.Ready
		t.Scheduled += ps.Scheduled
		t.InFlight += ps.InFlight
		t.Dead += ps.Dead
		t.Enqueued += ps.Enqueued
		t.Duplicates += ps.Duplicates
		t.Rejected += ps.Rejected
		t.Leased += ps.Leased
		t.Acked += ps.Acked
		t.Retried += ps.Retried
		t.DeadLettered += ps.DeadLettered
		t.Expired += ps.Expired
		t.Compactions += ps.Compactions
		t.LogBytes += ps.LogBytes
	}
	return t
}
