package partition

import (
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

// Record types. Values are persisted; never renumber.
const (
	recEnqueue    byte = 1  // job.Job (state Ready or Scheduled)
	recLease      byte = 2  // leaseRec
	recRenew      byte = 3  // renewRec
	recAck        byte = 4  // ackRec
	recRetry      byte = 5  // retryRec
	recDead       byte = 6  // deadRec
	recDelete     byte = 7  // deleteRec
	recReplay     byte = 8  // replayRec: DLQ -> ready
	recUpsert     byte = 9  // job.Job: snapshot of a live job
	recUpsertDead byte = 10 // job.DeadLetter: snapshot of a DLQ entry
	recIdem       byte = 11 // idemRec: snapshot of an idempotency entry
	recMeta       byte = 12 // metaRec: snapshot counters
)

type leaseRec struct {
	ID       string    `json:"id"`
	Attempt  int       `json:"attempt"`
	Worker   string    `json:"worker"`
	Deadline time.Time `json:"deadline"`
}

type renewRec struct {
	ID       string    `json:"id"`
	Attempt  int       `json:"attempt"`
	Deadline time.Time `json:"deadline"`
}

type ackRec struct {
	ID      string    `json:"id"`
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
}

type retryRec struct {
	ID      string    `json:"id"`
	Attempt int       `json:"attempt"`
	RunAt   time.Time `json:"run_at"`
	Err     string    `json:"err,omitempty"`
	At      time.Time `json:"at"`
}

type deadRec struct {
	ID      string    `json:"id"`
	Attempt int       `json:"attempt"`
	Reason  string    `json:"reason"`
	Err     string    `json:"err,omitempty"`
	Worker  string    `json:"worker,omitempty"`
	At      time.Time `json:"at"`
}

type deleteRec struct {
	ID string    `json:"id"`
	At time.Time `json:"at"`
}

type replayRec struct {
	ID  string    `json:"id"`
	Seq uint64    `json:"seq"`
	At  time.Time `json:"at"`
}

type idemRec struct {
	Key     string    `json:"key"`
	JobID   string    `json:"job_id"`
	Expires time.Time `json:"expires"`
}

// metaRec is written at the start of a snapshot. Besides the sequence counter
// it carries the cumulative totals of logged events: compaction discards the
// history those totals were counted from, so they must be preserved
// explicitly. (Duplicates, rejections and lease expiries are not logged and
// therefore restart from zero; they are per-process counters.)
type metaRec struct {
	Seq          uint64 `json:"seq"`
	Enqueued     uint64 `json:"enqueued,omitempty"`
	Leased       uint64 `json:"leased,omitempty"`
	Acked        uint64 `json:"acked,omitempty"`
	Retried      uint64 `json:"retried,omitempty"`
	DeadLettered uint64 `json:"dead_lettered,omitempty"`
}

// mustEncode marshals one of the plain record structs above. Marshalling
// them cannot fail as long as Job.Payload is valid JSON, which validate
// enforces, so a failure here is a programming error.
func mustEncode(t byte, v any) storage.Record {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("partition: encoding record type %d: %v", t, err))
	}
	return storage.Record{Type: t, Data: b}
}

func decode[T any](rec storage.Record) (T, error) {
	var v T
	if err := json.Unmarshal(rec.Data, &v); err != nil {
		return v, fmt.Errorf("partition: decoding record type %d: %w", rec.Type, err)
	}
	return v, nil
}

// applyRecord applies one persisted record during recovery.
func (p *Partition) applyRecord(rec storage.Record) error {
	switch rec.Type {
	case recEnqueue, recUpsert:
		j, err := decode[job.Job](rec)
		if err != nil {
			return err
		}
		p.putJob(j)
		if rec.Type == recEnqueue {
			p.stats.enqueued++ // replay is the only place logged events are re-counted
		}
	case recLease:
		r, err := decode[leaseRec](rec)
		if err != nil {
			return err
		}
		p.applyLease(r)
		p.stats.leased++
	case recRenew:
		r, err := decode[renewRec](rec)
		if err != nil {
			return err
		}
		p.applyRenew(r)
	case recAck:
		r, err := decode[ackRec](rec)
		if err != nil {
			return err
		}
		p.applyAck(r)
		p.stats.acked++
	case recRetry:
		r, err := decode[retryRec](rec)
		if err != nil {
			return err
		}
		p.applyRetry(r)
		p.stats.retried++
	case recDead:
		r, err := decode[deadRec](rec)
		if err != nil {
			return err
		}
		p.applyDead(r)
		p.stats.deadLettered++
	case recDelete:
		r, err := decode[deleteRec](rec)
		if err != nil {
			return err
		}
		p.applyDelete(r)
	case recReplay:
		r, err := decode[replayRec](rec)
		if err != nil {
			return err
		}
		p.applyReplay(r)
	case recUpsertDead:
		d, err := decode[job.DeadLetter](rec)
		if err != nil {
			return err
		}
		p.putDead(d)
	case recIdem:
		r, err := decode[idemRec](rec)
		if err != nil {
			return err
		}
		p.rememberIdem(r.Key, r.JobID, r.Expires)
	case recMeta:
		r, err := decode[metaRec](rec)
		if err != nil {
			return err
		}
		if r.Seq > p.seq {
			p.seq = r.Seq
		}
		p.stats.enqueued, p.stats.leased, p.stats.acked = r.Enqueued, r.Leased, r.Acked
		p.stats.retried, p.stats.deadLettered = r.Retried, r.DeadLettered
	default:
		return fmt.Errorf("partition: unknown record type %d", rec.Type)
	}
	return nil
}

// ---- state transitions ----
//
// The apply* functions are the only code that mutates canonical state
// (jobs, dead, idem, counts). They are pure with respect to time and
// randomness: every timestamp and delay they use is carried in the record.
// That is what makes replay reproduce exactly what the live path did.
// They do NOT touch the ready queues or the timer heap; those are derived
// indexes maintained by the callers (live path) or rebuilt after replay.

func (p *Partition) setState(j *job.Job, s job.State) {
	if j.State != 0 {
		p.counts[j.State]--
	}
	j.State = s
	p.counts[s]++
}

func (p *Partition) removeJob(j *job.Job) {
	p.counts[j.State]--
	delete(p.jobs, j.ID)
}

func (p *Partition) putJob(j job.Job) *job.Job {
	if old := p.jobs[j.ID]; old != nil {
		p.removeJob(old)
	}
	if j.Seq > p.seq {
		p.seq = j.Seq
	}
	st := j.State
	jp := &j
	jp.State = 0
	p.jobs[jp.ID] = jp
	p.setState(jp, st)
	if jp.IdempotencyKey != "" {
		p.rememberIdem(idemScope(jp), jp.ID, jp.CreatedAt.Add(p.cfg.IdempotencyTTL))
	}
	return jp
}

func (p *Partition) putDead(d job.DeadLetter) {
	if old := p.dead[d.OriginalJobID]; old == nil {
		p.counts[job.StateDead]++
	}
	p.dead[d.OriginalJobID] = &d
}

func (p *Partition) applyLease(r leaseRec) *job.Job {
	j := p.jobs[r.ID]
	if j == nil {
		return nil
	}
	j.Attempt = r.Attempt
	j.LeaseOwner = r.Worker
	j.VisibleAt = r.Deadline
	p.setState(j, job.StateInFlight)
	return j
}

func (p *Partition) applyRenew(r renewRec) *job.Job {
	j := p.jobs[r.ID]
	if j == nil {
		return nil
	}
	j.VisibleAt = r.Deadline
	return j
}

func (p *Partition) applyAck(r ackRec) {
	if j := p.jobs[r.ID]; j != nil {
		p.removeJob(j)
	}
}

func (p *Partition) applyRetry(r retryRec) *job.Job {
	j := p.jobs[r.ID]
	if j == nil {
		return nil
	}
	j.LeaseOwner = ""
	j.LastError = r.Err
	j.VisibleAt = r.RunAt
	p.setState(j, job.StateScheduled)
	return j
}

func (p *Partition) applyDead(r deadRec) {
	j := p.jobs[r.ID]
	if j == nil {
		return
	}
	worker := r.Worker
	if worker == "" {
		worker = j.LeaseOwner
	}
	dj := *j
	dj.State = job.StateDead
	dj.LeaseOwner = ""
	if r.Err != "" {
		dj.LastError = r.Err
	}
	p.removeJob(j)
	p.putDead(job.DeadLetter{
		Job:           dj,
		OriginalQueue: dj.Queue,
		OriginalJobID: dj.ID,
		Reason:        r.Reason,
		LastError:     dj.LastError,
		WorkerID:      worker,
		Attempts:      r.Attempt,
		DeadAt:        r.At,
	})
}

func (p *Partition) applyDelete(r deleteRec) {
	if j := p.jobs[r.ID]; j != nil {
		p.removeJob(j)
		return
	}
	if p.dead[r.ID] != nil {
		delete(p.dead, r.ID)
		p.counts[job.StateDead]--
	}
}

func (p *Partition) applyReplay(r replayRec) *job.Job {
	d := p.dead[r.ID]
	if d == nil {
		return nil
	}
	delete(p.dead, r.ID)
	p.counts[job.StateDead]--
	j := d.Job
	j.State = job.StateReady
	j.Attempt = 0
	j.LeaseOwner = ""
	j.VisibleAt = r.At
	j.Seq = r.Seq
	return p.putJob(j)
}

// ---- idempotency index ----

type idemEntry struct {
	jobID   string
	expires time.Time
}

type idemRef struct {
	key     string
	expires time.Time
}

func idemScope(j *job.Job) string {
	return j.Tenant + "\x00" + j.Queue + "\x00" + j.IdempotencyKey
}

func (p *Partition) rememberIdem(key, jobID string, expires time.Time) {
	p.idem[key] = idemEntry{jobID: jobID, expires: expires}
	p.idemOrder = append(p.idemOrder, idemRef{key: key, expires: expires})
}

// sweepIdemLocked drops expired keys from the front of the insertion-ordered
// list. TTL is constant, so insertion order is expiry order and the sweep is
// amortised O(1) per key; no full scan.
func (p *Partition) sweepIdemLocked(now time.Time) {
	for p.idemHead < len(p.idemOrder) {
		ref := p.idemOrder[p.idemHead]
		if ref.expires.After(now) {
			break
		}
		if e, ok := p.idem[ref.key]; ok && e.expires.Equal(ref.expires) {
			delete(p.idem, ref.key)
		}
		p.idemOrder[p.idemHead] = idemRef{}
		p.idemHead++
	}
	if p.idemHead == len(p.idemOrder) {
		p.idemOrder, p.idemHead = p.idemOrder[:0], 0
	} else if p.idemHead >= 1024 && p.idemHead*2 >= len(p.idemOrder) {
		n := copy(p.idemOrder, p.idemOrder[p.idemHead:])
		clear(p.idemOrder[n:])
		p.idemOrder, p.idemHead = p.idemOrder[:n], 0
	}
}

// ---- snapshot (compaction) ----

type snapshotState struct {
	jobs []job.Job
	dead []job.DeadLetter
	idem []idemRec
	seq  uint64
	meta metaRec
}

// captureLocked copies live state. Copies are shallow: Payload slices are
// immutable by convention so sharing them is safe, and the heavy JSON
// encoding happens later, outside the partition lock.
func (p *Partition) captureLocked(now time.Time) snapshotState {
	s := snapshotState{seq: p.seq, meta: metaRec{Seq: p.seq, Enqueued: p.stats.enqueued, Leased: p.stats.leased,
		Acked: p.stats.acked, Retried: p.stats.retried, DeadLettered: p.stats.deadLettered}}
	for _, j := range p.jobs {
		s.jobs = append(s.jobs, *j)
	}
	sort.Slice(s.jobs, func(a, b int) bool { return s.jobs[a].Seq < s.jobs[b].Seq })
	for _, d := range p.dead {
		s.dead = append(s.dead, *d)
	}
	sort.Slice(s.dead, func(a, b int) bool { return s.dead[a].DeadAt.Before(s.dead[b].DeadAt) })
	for k, e := range p.idem {
		if e.expires.After(now) {
			s.idem = append(s.idem, idemRec{Key: k, JobID: e.jobID, Expires: e.expires})
		}
	}
	sort.Slice(s.idem, func(a, b int) bool { return s.idem[a].Expires.Before(s.idem[b].Expires) })
	return s
}

func (s snapshotState) records() []storage.Record {
	recs := make([]storage.Record, 0, 1+len(s.jobs)+len(s.dead)+len(s.idem))
	recs = append(recs, mustEncode(recMeta, s.meta))
	for i := range s.jobs {
		recs = append(recs, mustEncode(recUpsert, s.jobs[i]))
	}
	for i := range s.dead {
		recs = append(recs, mustEncode(recUpsertDead, s.dead[i]))
	}
	for i := range s.idem {
		recs = append(recs, mustEncode(recIdem, s.idem[i]))
	}
	return recs
}
