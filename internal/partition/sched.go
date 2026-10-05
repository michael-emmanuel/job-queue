package partition

import (
	"container/heap"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/job"
)

// classOf maps a 1-10 priority to a scheduling class.
func classOf(priority int) int {
	switch {
	case priority >= 8:
		return classHigh
	case priority >= 4:
		return classNormal
	default:
		return classLow
	}
}

// fifo is an amortised O(1) queue of job IDs. It stores IDs, not jobs, so
// cancelled or superseded entries are skipped lazily on pop instead of being
// searched for and removed (which would be O(n)).
type fifo struct {
	buf  []string
	head int
}

func (q *fifo) push(id string) { q.buf = append(q.buf, id) }

func (q *fifo) pop() (string, bool) {
	if q.head >= len(q.buf) {
		return "", false
	}
	id := q.buf[q.head]
	q.buf[q.head] = ""
	q.head++
	switch {
	case q.head == len(q.buf):
		q.buf, q.head = q.buf[:0], 0
	case q.head >= 1024 && q.head*2 >= len(q.buf):
		n := copy(q.buf, q.buf[q.head:])
		clear(q.buf[n:])
		q.buf, q.head = q.buf[:n], 0
	}
	return id, true
}

func (q *fifo) reset() { q.buf, q.head = nil, 0 }

// timerKind distinguishes the two reasons a job needs to be looked at later.
type timerKind uint8

const (
	timerVisible timerKind = iota // Scheduled -> Ready at `at`
	timerLease                    // InFlight lease expires at `at`
)

type timerItem struct {
	at      time.Time
	id      string
	attempt int
	kind    timerKind
}

// timerHeap is a min-heap on `at`. It is the partition-local scheduler: one
// structure serves delayed jobs, retry backoff and visibility timeouts.
//
// Entries are never removed early. A renewed lease or a cancelled job simply
// leaves a stale entry that is recognised and dropped when it reaches the
// top, trading a little memory for O(log n) updates with no search.
type timerHeap []timerItem

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].at.Before(h[j].at) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *timerHeap) Push(x any)        { *h = append(*h, x.(timerItem)) }
func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = timerItem{}
	*h = old[:n-1]
	return it
}

// pickLocked chooses the next ready job by weighted fair queueing across
// priority classes.
//
// Each class holds `credits`; serving a class spends one. When no class that
// has work has credits left, all credits refill from the configured weights.
// Under contention the classes are served in the ratio of their weights
// (default 4:2:1), so a flood of high-priority jobs cannot starve the low
// class: it still gets 1/7 of dequeues. The scheduler is work-conserving: if
// only one class has jobs it receives everything.
func (p *Partition) pickLocked() *job.Job {
	for round := 0; round < 2; round++ {
		for c := 0; c < numClasses; c++ {
			if p.credits[c] == 0 {
				continue
			}
			if j := p.popClassLocked(c); j != nil {
				p.credits[c]--
				return j
			}
		}
		p.credits = p.cfg.Weights
	}
	return nil
}

func (p *Partition) popClassLocked(c int) *job.Job {
	for {
		id, ok := p.ready[c].pop()
		if !ok {
			return nil
		}
		// Stale entries (cancelled, already leased through a duplicate
		// entry, or no longer ready) are dropped here.
		if j := p.jobs[id]; j != nil && j.State == job.StateReady {
			return j
		}
	}
}

func (p *Partition) pushReadyLocked(j *job.Job) {
	p.ready[classOf(j.Priority)].push(j.ID)
	p.notifyLocked()
}

func (p *Partition) pushTimerLocked(it timerItem) {
	earliest := len(p.timers) == 0 || it.at.Before(p.timers[0].at)
	pushHeap(&p.timers, it)
	if earliest {
		select { // wake the Run loop so it re-arms for the new earliest deadline
		case p.wake <- struct{}{}:
		default:
		}
	}
}

// notifyLocked wakes every goroutine long-polling in Dequeue. A closed
// channel is the broadcast primitive here: a sync.Cond cannot be combined
// with a context or a timeout in a select.
func (p *Partition) notifyLocked() {
	if p.avail != nil {
		close(p.avail)
		p.avail = nil
	}
	if p.cfg.OnReady != nil {
		p.cfg.OnReady()
	}
}

func (p *Partition) availChLocked() <-chan struct{} {
	if p.avail == nil {
		p.avail = make(chan struct{})
	}
	return p.avail
}

func pushHeap(h *timerHeap, it timerItem) { heap.Push(h, it) }
func popHeap(h *timerHeap) timerItem      { return heap.Pop(h).(timerItem) }
