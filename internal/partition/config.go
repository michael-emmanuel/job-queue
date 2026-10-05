// Package partition implements a single queue partition: the unit of
// ownership, locking, persistence and (later) replication.
//
// A Partition holds the authoritative in-memory state of its jobs and a
// write-ahead Log that is the source of truth. Every state change is expressed
// as a log record, appended first, then applied by the same deterministic
// apply functions that recovery uses. That is event sourcing, and it is what
// will let followers replay a leader's records in the replication milestone.
package partition

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/michael-emmanuel/jobqueue/internal/clock"
	"github.com/michael-emmanuel/jobqueue/internal/retry"
	"github.com/michael-emmanuel/jobqueue/internal/storage"
)

// Errors returned by partition operations.
var (
	ErrNotFound   = errors.New("partition: job not found")
	ErrStaleLease = errors.New("partition: stale lease")
	ErrQueueFull  = errors.New("partition: queue full")
	ErrInvalidJob = errors.New("partition: invalid job")
	ErrClosed     = errors.New("partition: closed")
)

// Number of priority classes and their indexes. Priorities 8-10 are high,
// 4-7 normal, 1-3 low. See classOf.
const (
	classHigh = iota
	classNormal
	classLow
	numClasses
)

// Config configures a Partition. Zero values get defaults from setDefaults.
type Config struct {
	Name string // for logs, e.g. "email/0"
	Log  storage.Log
	// Clock defaults to the wall clock.
	Clock clock.Clock
	Retry retry.Policy
	// Rand supplies jitter randomness in [0,1); nil uses math/rand/v2.
	Rand func() float64

	// Weights are the dequeue shares of the high, normal and low priority
	// classes under contention. Default 4:2:1.
	Weights [numClasses]int

	DefaultVisibility time.Duration // default 30s
	MaxVisibility     time.Duration // upper bound for any lease or renewal; default 15m

	// MaxDepth bounds live (ready + scheduled + in-flight) jobs: the
	// backpressure limit. Enqueue returns ErrQueueFull beyond it. Default 100000.
	MaxDepth        int
	MaxPayloadBytes int // default 256 KiB

	// IdempotencyTTL is how long an idempotency key suppresses duplicates.
	IdempotencyTTL time.Duration // default 24h

	// CompactMinBytes triggers automatic compaction once the log exceeds it
	// and has doubled since the last compaction. 0 disables automatic compaction.
	CompactMinBytes int64

	// OnReady, if set, is called (with the partition lock held, so it must be
	// cheap and must not call back into the partition) whenever jobs become
	// ready. A queue spanning several partitions uses it to wake long-polling
	// consumers that are waiting on any of them.
	OnReady func()

	Logger *slog.Logger
}

func (c *Config) setDefaults() error {
	if c.Log == nil {
		return errors.New("partition: Config.Log is required")
	}
	if c.Clock == nil {
		c.Clock = clock.Real{}
	}
	if c.Retry == (retry.Policy{}) {
		c.Retry = retry.DefaultPolicy()
	}
	if err := c.Retry.Validate(); err != nil {
		return err
	}
	if c.Weights == [numClasses]int{} {
		c.Weights = [numClasses]int{4, 2, 1}
	}
	for i, w := range c.Weights {
		if w < 1 {
			return fmt.Errorf("partition: Weights[%d] must be >= 1", i)
		}
	}
	if c.DefaultVisibility <= 0 {
		c.DefaultVisibility = 30 * time.Second
	}
	if c.MaxVisibility <= 0 {
		c.MaxVisibility = 15 * time.Minute
	}
	if c.DefaultVisibility > c.MaxVisibility {
		return errors.New("partition: DefaultVisibility exceeds MaxVisibility")
	}
	if c.MaxDepth <= 0 {
		c.MaxDepth = 100_000
	}
	if c.MaxPayloadBytes <= 0 {
		c.MaxPayloadBytes = 256 << 10
	}
	if c.IdempotencyTTL <= 0 {
		c.IdempotencyTTL = 24 * time.Hour
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	c.Logger = c.Logger.With("partition", c.Name)
	return nil
}
