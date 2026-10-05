// Package job defines the domain model shared by every component: the Job,
// its lifecycle states, and the dead-letter record.
package job

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// State is the position of a job in its lifecycle.
//
//	Scheduled --(run_at reached)--> Ready --(lease)--> InFlight
//	InFlight --(ack)--> Complete (record removed)
//	InFlight --(fail, attempts left)--> Scheduled (backoff) --> Ready
//	InFlight --(visibility timeout, attempts left)--> Ready
//	InFlight --(fail non-retryable | attempts exhausted)--> Dead (DLQ)
type State uint8

const (
	StateReady State = iota + 1
	StateScheduled
	StateInFlight
	StateDead
	// StateComplete is reported for jobs that finished; the partition does
	// not retain completed jobs, so it never appears in stored state.
	StateComplete
)

var stateNames = [...]string{"invalid", "ready", "scheduled", "in_flight", "dead", "complete"}

func (s State) String() string {
	if int(s) < len(stateNames) {
		return stateNames[s]
	}
	return "invalid"
}

// MarshalText implements encoding.TextMarshaler so states are readable in
// the log and in API responses.
func (s State) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// UnmarshalText implements encoding.TextUnmarshaler.
//
// The zero value (a job that has not been enqueued yet, for example one a
// client sends to a broker) round-trips as "invalid" so that every Job can
// cross a process boundary.
func (s *State) UnmarshalText(b []byte) error {
	if string(b) == stateNames[0] || len(b) == 0 {
		*s = 0
		return nil
	}
	for i, n := range stateNames {
		if n == string(b) {
			*s = State(i)
			return nil
		}
	}
	return fmt.Errorf("job: unknown state %q", b)
}

// Priority bounds. Higher number = more urgent.
const (
	MinPriority     = 1
	DefaultPriority = 5
	MaxPriority     = 10
)

// Job is a unit of work. Fields before the divider are supplied by the
// producer; fields after it are owned by the partition.
type Job struct {
	ID             string          `json:"job_id"`
	Tenant         string          `json:"tenant,omitempty"`
	Queue          string          `json:"queue"`
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload,omitempty"`
	Priority       int             `json:"priority"`
	PartitionKey   string          `json:"partition_key,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	RunAt          time.Time       `json:"run_at"`

	// ---- owned by the partition ----

	CreatedAt time.Time `json:"created_at"`
	// Seq is a partition-local monotonically increasing number that gives
	// best-effort FIFO order and survives restarts.
	Seq   uint64 `json:"seq"`
	State State  `json:"state"`
	// Attempt counts deliveries. It is incremented on every lease and doubles
	// as the fencing token: acks carrying a stale attempt are rejected.
	Attempt int `json:"attempt"`
	// VisibleAt is the time the job next changes state on its own: the
	// visibility deadline when in flight, the release time when scheduled.
	VisibleAt time.Time `json:"visible_at"`
	// LeaseOwner is non-empty while the current attempt's lease has not been
	// released by an ack, a failure report or a re-lease. A lapsed but
	// unclaimed lease keeps its owner, which is what lets a late ack succeed.
	LeaseOwner string `json:"lease_owner,omitempty"`
	LastError  string `json:"last_error,omitempty"`
}

// Dead-letter reasons.
const (
	ReasonMaxAttempts         = "max_attempts_exceeded"
	ReasonNonRetryable        = "non_retryable_error"
	ReasonVisibilityExhausted = "visibility_timeout_exhausted"
)

// DeadLetter is the DLQ record: the job plus the operational metadata needed
// to decide whether to replay or discard it.
type DeadLetter struct {
	Job           Job       `json:"job"`
	OriginalQueue string    `json:"original_queue"`
	OriginalJobID string    `json:"original_job_id"`
	Reason        string    `json:"reason"`
	LastError     string    `json:"last_error,omitempty"`
	WorkerID      string    `json:"worker_id,omitempty"`
	Attempts      int       `json:"attempts"`
	DeadAt        time.Time `json:"dead_at"`
}

// NewID returns a 128-bit identifier: 64 bits of millisecond timestamp
// followed by 64 random bits, hex encoded. IDs are roughly time-sortable and
// collision-resistant without coordination.
func NewID() string {
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], uint64(time.Now().UnixMilli()))
	if _, err := rand.Read(b[8:]); err != nil {
		// No entropy means no safe way to mint identifiers.
		panic("job: crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
