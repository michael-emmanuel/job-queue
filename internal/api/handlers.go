package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/broker"
	"github.com/michael-emmanuel/jobqueue/internal/job"
	"github.com/michael-emmanuel/jobqueue/internal/partition"
)

// jobView is the wire form of a job. Payloads are included only where the
// caller needs them to do work (deliveries) or to triage (DLQ), not in
// status lookups.
type jobView struct {
	JobID              string          `json:"job_id"`
	Queue              string          `json:"queue"`
	Tenant             string          `json:"tenant,omitempty"`
	Type               string          `json:"type"`
	Payload            json.RawMessage `json:"payload,omitempty"`
	Priority           int             `json:"priority"`
	State              string          `json:"state"`
	Attempt            int             `json:"attempt"`
	MaxAttempts        int             `json:"max_attempts"`
	CreatedAt          time.Time       `json:"created_at"`
	RunAt              *time.Time      `json:"run_at,omitempty"`
	VisibilityDeadline *time.Time      `json:"visibility_deadline,omitempty"`
	LastError          string          `json:"last_error,omitempty"`
}

func viewOf(j job.Job, payload bool) jobView {
	v := jobView{JobID: j.ID, Queue: j.Queue, Tenant: j.Tenant, Type: j.Type, Priority: j.Priority,
		State: j.State.String(), Attempt: j.Attempt, MaxAttempts: j.MaxAttempts,
		CreatedAt: j.CreatedAt, LastError: j.LastError}
	if payload {
		v.Payload = j.Payload
	}
	if !j.RunAt.IsZero() {
		t := j.RunAt
		v.RunAt = &t
	}
	if j.State == job.StateInFlight {
		t := j.VisibleAt
		v.VisibilityDeadline = &t
	}
	return v
}

// ---- submit / inspect / cancel ----

type submitRequest struct {
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	PartitionKey   string          `json:"partition_key"`
	IdempotencyKey string          `json:"idempotency_key"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          *time.Time      `json:"run_at"`
}

type submitResponse struct {
	JobID     string `json:"job_id"`
	Queue     string `json:"queue"`
	State     string `json:"state"`
	Duplicate bool   `json:"duplicate"`
}

func (s *Server) submitJob(w http.ResponseWriter, r *http.Request, p *Principal) error {
	var req submitRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if !broker.ValidName(req.Queue) {
		return newErr(http.StatusBadRequest, "invalid_queue", "queue is required and must match [a-z0-9][a-z0-9_-]{0,62}")
	}
	if len(req.Type) > 128 || len(req.PartitionKey) > 255 {
		return newErr(http.StatusBadRequest, "invalid_job", "type must be <= 128 and partition_key <= 255 bytes")
	}
	// The Idempotency-Key header is the conventional place; the body field
	// exists for clients that cannot set headers. They must agree.
	idem := req.IdempotencyKey
	if h := r.Header.Get("Idempotency-Key"); h != "" {
		if idem != "" && idem != h {
			return newErr(http.StatusBadRequest, "invalid_job", "Idempotency-Key header and idempotency_key field differ")
		}
		idem = h
	}
	if len(idem) > 255 {
		return newErr(http.StatusBadRequest, "invalid_job", "idempotency key must be <= 255 bytes")
	}
	ri := infoFrom(r.Context())
	ri.queue = req.Queue

	// Admission order: tenant, then global. Checking the tenant first means a
	// tenant that is over its own limit is turned away without touching the
	// shared budget, so it cannot burn capacity that belongs to others.
	tb := s.tenants.Get(p.Tenant)
	if ok, wait := tb.Take(1); !ok {
		return rateLimited("tenant", wait)
	}
	if ok, wait := s.global.Take(1); !ok {
		tb.Refund(1)
		return rateLimited("global", wait)
	}
	refund := func() { tb.Refund(1); s.global.Refund(1) }

	j := job.Job{
		Tenant: p.Tenant, Type: req.Type, Payload: req.Payload, Priority: req.Priority,
		PartitionKey: req.PartitionKey, IdempotencyKey: idem, MaxAttempts: req.MaxAttempts,
	}
	state := job.StateReady
	if req.RunAt != nil {
		j.RunAt = *req.RunAt
		if j.RunAt.After(s.o.Clock.Now()) {
			state = job.StateScheduled
		}
	}
	res, err := s.be.Enqueue(r.Context(), req.Queue, j)
	if res.ID != "" {
		ri.jobID = res.ID
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			// The job may exist: the partition accepted it and only the wait
			// for durability was interrupted. Do not refund; the client will
			// retry with the same key and get the original job back.
			return err
		}
		refund()
		return err
	}
	status := http.StatusCreated
	if res.Duplicate {
		status = http.StatusOK
	}
	s.writeJSON(w, status, submitResponse{JobID: res.ID, Queue: req.Queue, State: state.String(), Duplicate: res.Duplicate})
	return nil
}

// visibleTo reports whether the principal may see a job. Producers see only
// their own tenant's jobs; workers and admins see any. A foreign job is
// reported as not found, not forbidden, so job IDs cannot be probed for
// existence across tenants.
func visibleTo(p *Principal, j job.Job) bool {
	return p.Has(RoleWorker) || j.Tenant == p.Tenant
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id := r.PathValue("id")
	ri := infoFrom(r.Context())
	ri.jobID = id
	if q, _, err := broker.ParseID(id); err == nil {
		ri.queue = q
	}
	j, err := s.be.Get(r.Context(), id)
	if err != nil {
		return err
	}
	if !visibleTo(p, j) {
		return partition.ErrNotFound
	}
	s.writeJSON(w, http.StatusOK, viewOf(j, false))
	return nil
}

func (s *Server) cancelJob(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id := r.PathValue("id")
	ri := infoFrom(r.Context())
	ri.jobID = id
	j, err := s.be.Get(r.Context(), id)
	if err != nil {
		return err
	}
	ri.queue = j.Queue
	if j.Tenant != p.Tenant && !p.Has(RoleAdmin) {
		return partition.ErrNotFound
	}
	if j.State == job.StateDead {
		return newErr(http.StatusConflict, "job_dead", "the job is in the dead-letter queue; delete it there")
	}
	if err := s.be.Cancel(r.Context(), id); err != nil {
		return err
	}
	s.audit(r, p, "cancel_job", "job_id", id, "queue", j.Queue)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ---- worker protocol ----

type pullRequest struct {
	Worker     string          `json:"worker"`
	Group      string          `json:"group"`
	Max        int             `json:"max"`
	Visibility broker.Duration `json:"visibility"`
	Wait       broker.Duration `json:"wait"`
}

type pullResponse struct {
	Jobs []jobView `json:"jobs"`
}

func (s *Server) pull(w http.ResponseWriter, r *http.Request, p *Principal) error {
	var req pullRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if req.Worker == "" || len(req.Worker) > 128 {
		return newErr(http.StatusBadRequest, "invalid_request", "worker is required (<= 128 bytes)")
	}
	if req.Max < 0 || req.Visibility < 0 || req.Wait < 0 {
		return newErr(http.StatusBadRequest, "invalid_request", "max, visibility and wait must not be negative")
	}
	name := r.PathValue("queue")
	infoFrom(r.Context()).queue = name
	wait := min(req.Wait.Std(), s.o.MaxLongPoll)

	// Tie the poll to the drain signal so shutdown does not wait out a
	// 30-second long poll.
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	stop := context.AfterFunc(s.drainCtx, cancel)
	defer stop()

	jobs, err := s.be.Pull(ctx, name, PullArgs{Worker: req.Worker, Group: req.Group, Max: req.Max,
		Visibility: req.Visibility.Std(), Wait: wait})
	result := "empty"
	switch {
	case err == nil && len(jobs) > 0:
		result = "jobs"
	case err != nil && errors.Is(err, context.Canceled) && s.draining() && r.Context().Err() == nil:
		err = nil // shutdown release: an empty poll, not an error
		result = "drained"
	case err != nil:
		var rl *broker.RateLimitedError
		if errors.As(err, &rl) {
			result = "rate_limited"
		} else {
			result = "error"
		}
	}
	s.m.pulls.With(name, result).Inc()
	if err != nil {
		return err
	}
	now := s.o.Clock.Now()
	out := pullResponse{Jobs: make([]jobView, 0, len(jobs))}
	for _, j := range jobs {
		if j.Attempt == 1 {
			s.m.queueDelay.With(name).Observe(now.Sub(j.CreatedAt).Seconds())
		}
		out.Jobs = append(out.Jobs, viewOf(j, true))
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

type attemptRequest struct {
	Attempt int `json:"attempt"`
}

func (s *Server) jobRef(r *http.Request) (string, error) {
	id := r.PathValue("id")
	ri := infoFrom(r.Context())
	ri.jobID = id
	queue, _, err := broker.ParseID(id)
	if err != nil {
		return "", err
	}
	ri.queue = queue
	return id, nil
}

func (s *Server) ack(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id, err := s.jobRef(r)
	if err != nil {
		return err
	}
	var req attemptRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if err := s.be.Ack(r.Context(), id, req.Attempt); err != nil {
		return err
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

type failRequest struct {
	Attempt   int    `json:"attempt"`
	Error     string `json:"error"`
	Retryable *bool  `json:"retryable"` // default true
}

func (s *Server) fail(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id, err := s.jobRef(r)
	if err != nil {
		return err
	}
	var req failRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	msg := req.Error
	if len(msg) > 2048 { // bound what a worker can make the DLQ retain
		msg = msg[:2048]
	}
	out, err := s.be.Fail(r.Context(), id, req.Attempt, partition.Failure{
		Err: msg, NonRetryable: req.Retryable != nil && !*req.Retryable,
	})
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]string{"outcome": string(out)})
	return nil
}

type renewRequest struct {
	Attempt int             `json:"attempt"`
	Extend  broker.Duration `json:"extend"`
}

func (s *Server) renew(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id, err := s.jobRef(r)
	if err != nil {
		return err
	}
	var req renewRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	if req.Extend < 0 {
		return newErr(http.StatusBadRequest, "invalid_request", "extend must not be negative")
	}
	dl, err := s.be.Renew(r.Context(), id, req.Attempt, req.Extend.Std())
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]time.Time{"visibility_deadline": dl})
	return nil
}

// ---- queue administration ----

type createQueueRequest struct {
	Name string `json:"name"`
	broker.QueueConfig
}

func (s *Server) createQueue(w http.ResponseWriter, r *http.Request, p *Principal) error {
	var req createQueueRequest
	if err := decodeJSON(r, &req); err != nil {
		return err
	}
	cfg, err := s.be.CreateQueue(r.Context(), req.Name, req.QueueConfig)
	if err != nil {
		return err
	}
	infoFrom(r.Context()).queue = req.Name
	s.audit(r, p, "create_queue", "queue", req.Name, "partitions", cfg.Partitions)
	s.writeJSON(w, http.StatusCreated, broker.QueueInfo{Name: req.Name, Config: cfg})
	return nil
}

func (s *Server) listQueues(w http.ResponseWriter, r *http.Request, p *Principal) error {
	qs, err := s.be.ListQueues(r.Context())
	if err != nil {
		return err
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"queues": qs})
	return nil
}

type statsView struct {
	Ready        int    `json:"ready"`
	Scheduled    int    `json:"scheduled"`
	InFlight     int    `json:"in_flight"`
	Dead         int    `json:"dead"`
	Enqueued     uint64 `json:"enqueued"`
	Duplicates   uint64 `json:"duplicates"`
	Rejected     uint64 `json:"rejected"`
	Leased       uint64 `json:"leased"`
	Acked        uint64 `json:"acked"`
	Retried      uint64 `json:"retried"`
	DeadLettered uint64 `json:"dead_lettered"`
	LeaseExpired uint64 `json:"lease_expired"`
	LogBytes     int64  `json:"log_bytes"`
}

func statsOf(s partition.Stats) statsView {
	return statsView{s.Ready, s.Scheduled, s.InFlight, s.Dead, s.Enqueued, s.Duplicates, s.Rejected,
		s.Leased, s.Acked, s.Retried, s.DeadLettered, s.Expired, s.LogBytes}
}

func (s *Server) getQueue(w http.ResponseWriter, r *http.Request, p *Principal) error {
	name := r.PathValue("queue")
	infoFrom(r.Context()).queue = name
	st, err := s.be.QueueStats(r.Context(), name)
	if err != nil {
		return err
	}
	out := struct {
		Name       string             `json:"name"`
		Config     broker.QueueConfig `json:"config"`
		Total      statsView          `json:"total"`
		Partitions []statsView        `json:"partitions"`
	}{Name: name, Config: st.Config, Total: statsOf(st.Total)}
	for _, ps := range st.Partitions {
		out.Partitions = append(out.Partitions, statsOf(ps))
	}
	s.writeJSON(w, http.StatusOK, out)
	return nil
}

func (s *Server) deleteQueue(w http.ResponseWriter, r *http.Request, p *Principal) error {
	name := r.PathValue("queue")
	infoFrom(r.Context()).queue = name
	if err := s.be.DeleteQueue(r.Context(), name); err != nil {
		return err
	}
	s.audit(r, p, "delete_queue", "queue", name)
	w.WriteHeader(http.StatusNoContent)
	return nil
}

// ---- dead-letter queue ----

type deadView struct {
	JobID     string          `json:"job_id"`
	Queue     string          `json:"original_queue"`
	Tenant    string          `json:"tenant"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	Reason    string          `json:"reason"`
	LastError string          `json:"last_error,omitempty"`
	WorkerID  string          `json:"worker_id,omitempty"`
	Attempts  int             `json:"attempts"`
	CreatedAt time.Time       `json:"created_at"`
	DeadAt    time.Time       `json:"dead_at"`
}

func (s *Server) listDLQ(w http.ResponseWriter, r *http.Request, p *Principal) error {
	name := r.PathValue("queue")
	infoFrom(r.Context()).queue = name
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			return newErr(http.StatusBadRequest, "invalid_request", "limit must be between 1 and 1000")
		}
		limit = n
	}
	dead, err := s.be.DeadLetters(r.Context(), name, limit)
	if err != nil {
		return err
	}
	out := make([]deadView, 0)
	for _, d := range dead {
		out = append(out, deadView{JobID: d.OriginalJobID, Queue: d.OriginalQueue, Tenant: d.Job.Tenant,
			Type: d.Job.Type, Payload: d.Job.Payload, Reason: d.Reason, LastError: d.LastError,
			WorkerID: d.WorkerID, Attempts: d.Attempts, CreatedAt: d.Job.CreatedAt, DeadAt: d.DeadAt})
	}
	s.writeJSON(w, http.StatusOK, map[string]any{"dead_letters": out})
	return nil
}

// dlqJobID validates that the job belongs to the queue named in the path, so
// /queues/a/dlq/<job of queue b>/replay cannot act across queues.
func (s *Server) dlqJobID(r *http.Request) (string, error) {
	name, id := r.PathValue("queue"), r.PathValue("id")
	ri := infoFrom(r.Context())
	ri.queue, ri.jobID = name, id
	queue, _, err := broker.ParseID(id)
	if err != nil {
		return "", err
	}
	if queue != name {
		return "", partition.ErrNotFound
	}
	return id, nil
}

func (s *Server) replayDLQ(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id, err := s.dlqJobID(r)
	if err != nil {
		return err
	}
	if err := s.be.ReplayDead(r.Context(), id); err != nil {
		return err
	}
	s.audit(r, p, "replay_dlq", "job_id", id)
	s.writeJSON(w, http.StatusOK, map[string]string{"job_id": id, "state": "ready"})
	return nil
}

func (s *Server) deleteDLQ(w http.ResponseWriter, r *http.Request, p *Principal) error {
	id, err := s.dlqJobID(r)
	if err != nil {
		return err
	}
	if err := s.be.DeleteDead(r.Context(), id); err != nil {
		return err
	}
	s.audit(r, p, "delete_dlq", "job_id", id)
	w.WriteHeader(http.StatusNoContent)
	return nil
}
