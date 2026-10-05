// Package broker owns queues: it creates and persists them, routes jobs to
// partitions, fans consumers out across partitions and applies per-queue
// limits. It is the single-node broker; the distribution milestone layers
// replication and consumer groups on top of the same Queue type.
package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/partition"
	"github.com/michael-emmanuel/jobqueue/internal/retry"
)

// Duration is a time.Duration that marshals as a string such as "30s".
type Duration time.Duration

// Std converts to time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// UnmarshalJSON implements json.Unmarshaler. Only strings like "250ms" are
// accepted: a bare number would be ambiguous between seconds and nanoseconds.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New(`duration must be a string such as "30s"`)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q", s)
	}
	*d = Duration(v)
	return nil
}

// RetryConfig is the JSON form of retry.Policy.
type RetryConfig struct {
	MaxAttempts int      `json:"max_attempts"`
	BaseDelay   Duration `json:"base_delay"`
	MaxDelay    Duration `json:"max_delay"`
	Jitter      string   `json:"jitter"` // "none", "equal" or "full"
}

func (r RetryConfig) policy() (retry.Policy, error) {
	p := retry.Policy{MaxAttempts: r.MaxAttempts, BaseDelay: r.BaseDelay.Std(), MaxDelay: r.MaxDelay.Std()}
	switch r.Jitter {
	case "none":
		p.Jitter = retry.JitterNone
	case "equal":
		p.Jitter = retry.JitterEqual
	case "full":
		p.Jitter = retry.JitterFull
	default:
		return p, fmt.Errorf("retry.jitter must be none, equal or full, got %q", r.Jitter)
	}
	return p, p.Validate()
}

// QueueConfig is the per-queue configuration. The partition count is fixed
// at creation: changing it would remap keys to different partitions.
type QueueConfig struct {
	Partitions        int         `json:"partitions"`
	DefaultVisibility Duration    `json:"default_visibility"`
	MaxVisibility     Duration    `json:"max_visibility"`
	MaxDepth          int         `json:"max_depth"` // queue-wide backlog bound, split evenly across partitions
	MaxPayloadBytes   int         `json:"max_payload_bytes"`
	IdempotencyTTL    Duration    `json:"idempotency_ttl"`
	Retry             RetryConfig `json:"retry"`
	// EnqueueRate and ConsumeRate are jobs per second for the whole queue;
	// 0 means unlimited. Bursts default to one second of rate.
	// ReplicationFactor and Ack apply to clustered queues: how many copies
	// of each partition exist and how many must hold a write before it is
	// acknowledged ("quorum", "leader" or "all"). Ignored by the single-node
	// broker.
	ReplicationFactor int     `json:"replication_factor,omitempty"`
	Ack               string  `json:"ack,omitempty"`
	EnqueueRate       float64 `json:"enqueue_rate"`
	EnqueueBurst      int     `json:"enqueue_burst"`
	ConsumeRate       float64 `json:"consume_rate"`
	ConsumeBurst      int     `json:"consume_burst"`
}

// DefaultQueueConfig returns the defaults applied to unspecified fields.
func DefaultQueueConfig() QueueConfig {
	return QueueConfig{
		Partitions:        4,
		DefaultVisibility: Duration(30 * time.Second),
		MaxVisibility:     Duration(15 * time.Minute),
		MaxDepth:          100_000,
		MaxPayloadBytes:   256 << 10,
		IdempotencyTTL:    Duration(24 * time.Hour),
		Retry: RetryConfig{MaxAttempts: 5, BaseDelay: Duration(time.Second),
			MaxDelay: Duration(5 * time.Minute), Jitter: "equal"},
	}
}

// withDefaults fills every unset field from d.
func (c QueueConfig) withDefaults(d QueueConfig) QueueConfig {
	if c.Partitions == 0 {
		c.Partitions = d.Partitions
	}
	if c.DefaultVisibility == 0 {
		c.DefaultVisibility = d.DefaultVisibility
	}
	if c.MaxVisibility == 0 {
		c.MaxVisibility = d.MaxVisibility
	}
	if c.MaxDepth == 0 {
		c.MaxDepth = d.MaxDepth
	}
	if c.MaxPayloadBytes == 0 {
		c.MaxPayloadBytes = d.MaxPayloadBytes
	}
	if c.IdempotencyTTL == 0 {
		c.IdempotencyTTL = d.IdempotencyTTL
	}
	if c.Retry.MaxAttempts == 0 {
		c.Retry.MaxAttempts = d.Retry.MaxAttempts
	}
	if c.Retry.BaseDelay == 0 {
		c.Retry.BaseDelay = d.Retry.BaseDelay
	}
	if c.Retry.MaxDelay == 0 {
		c.Retry.MaxDelay = d.Retry.MaxDelay
	}
	if c.Retry.Jitter == "" {
		c.Retry.Jitter = d.Retry.Jitter
	}
	return c
}

func (c QueueConfig) validate() error {
	switch {
	case c.Partitions < 1 || c.Partitions > 256:
		return errors.New("partitions must be between 1 and 256")
	case c.DefaultVisibility <= 0 || c.MaxVisibility < c.DefaultVisibility:
		return errors.New("visibility must satisfy 0 < default_visibility <= max_visibility")
	case c.MaxDepth < c.Partitions:
		return errors.New("max_depth must be at least the partition count")
	case c.MaxPayloadBytes < 1:
		return errors.New("max_payload_bytes must be positive")
	case c.IdempotencyTTL <= 0:
		return errors.New("idempotency_ttl must be positive")
	case c.ReplicationFactor < 0 || c.ReplicationFactor > 7:
		return errors.New("replication_factor must be between 0 (default) and 7")
	case c.Ack != "" && c.Ack != "quorum" && c.Ack != "leader" && c.Ack != "all":
		return errors.New(`ack must be "quorum", "leader" or "all"`)
	case c.EnqueueRate < 0 || c.ConsumeRate < 0 || c.EnqueueBurst < 0 || c.ConsumeBurst < 0:
		return errors.New("rates and bursts must not be negative")
	}
	_, err := c.Retry.policy()
	return err
}

// WithDefaults fills unset fields from DefaultQueueConfig.
func (c QueueConfig) WithDefaults() QueueConfig { return c.withDefaults(DefaultQueueConfig()) }

// Normalize fills unset fields from defaults and validates the result.
func (c QueueConfig) Normalize(defaults QueueConfig) (QueueConfig, error) {
	c = c.withDefaults(defaults)
	return c, c.validate()
}

// PartitionConfig builds the engine configuration for one partition of a
// queue with this config. The caller supplies Log, Clock, Logger and any
// hooks. MaxDepth is split evenly across partitions.
func (c QueueConfig) PartitionConfig(queue string, idx int) (partition.Config, error) {
	pol, err := c.Retry.policy()
	if err != nil {
		return partition.Config{}, err
	}
	return partition.Config{
		Name:              fmt.Sprintf("%s/%d", queue, idx),
		Retry:             pol,
		DefaultVisibility: c.DefaultVisibility.Std(),
		MaxVisibility:     c.MaxVisibility.Std(),
		MaxDepth:          (c.MaxDepth + c.Partitions - 1) / c.Partitions,
		MaxPayloadBytes:   c.MaxPayloadBytes,
		IdempotencyTTL:    c.IdempotencyTTL.Std(),
	}, nil
}

var nameRE = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,62}$`)

// ValidName reports whether s is a legal queue name. Dots are excluded
// because job IDs are "<queue>.<partition>.<id>".
func ValidName(s string) bool { return nameRE.MatchString(s) }
