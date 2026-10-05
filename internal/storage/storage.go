// Package storage defines the durable log abstraction used by partitions and
// provides a segmented, checksummed write-ahead log (WAL).
//
// The log is deliberately dumb: it stores opaque typed records in order and
// knows nothing about jobs. The partition owns the meaning of records, which
// keeps the storage backend replaceable (see docs/design-notes-m1.md).
package storage

import (
	"context"
	"errors"
)

// Record is one entry in the log. Type 0xFF is reserved for the log itself.
type Record struct {
	Type byte
	Data []byte
}

// snapshotMark is the first record of a snapshot segment. Recovery starts at
// the newest segment that begins with it and ignores everything older.
const snapshotMark byte = 0xFF

// WaitFunc blocks until a previously appended record is durable according to
// the log's sync policy, or until ctx is done.
//
// Splitting Append from Wait is what makes group commit possible: callers
// append under their own lock (fixing the order of records), release that
// lock, and only then wait for the fsync. Many callers share one fsync.
type WaitFunc func(ctx context.Context) error

var (
	// ErrClosed is returned by operations on a closed (or aborted) log.
	ErrClosed = errors.New("storage: log closed")
	// ErrCorrupt means a record failed validation somewhere it cannot be
	// safely discarded (anywhere except the tail of the newest segment).
	ErrCorrupt = errors.New("storage: corrupt log")
	// ErrReservedType is returned when a caller appends the reserved type.
	ErrReservedType = errors.New("storage: record type 0xFF is reserved")
)

// Log is an ordered, durable record store.
type Log interface {
	// Append adds rec to the log in call order and returns a WaitFunc that
	// resolves when it is durable. The record is in the log's order as soon
	// as Append returns, even if the WaitFunc is never called.
	Append(rec Record) (WaitFunc, error)

	// Replay streams every live record, oldest first, to fn. It must be called
	// before the first Append.
	Replay(fn func(Record) error) error

	// Rotate seals the active segment and returns a marker identifying
	// everything appended so far. It is the first half of compaction.
	Rotate() (uint64, error)

	// WriteSnapshot atomically replaces everything up to and including marker
	// with recs. It is the second half of compaction. Records appended after
	// Rotate are preserved.
	WriteSnapshot(marker uint64, recs []Record) error

	// Size returns the total bytes currently held by the log.
	Size() int64

	// Close flushes, makes everything durable and releases resources.
	Close() error
}
