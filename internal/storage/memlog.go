package storage

import (
	"context"
	"sync"
)

// MemLog is a non-durable Log for unit tests and benchmarks that want to
// measure the engine without disk cost. It is also the "V1" of the design:
// an in-memory queue with no persistence.
type MemLog struct {
	mu     sync.Mutex
	recs   []Record
	size   int64
	closed bool
}

var _ Log = (*MemLog)(nil)

// NewMemLog returns an empty MemLog.
func NewMemLog() *MemLog { return &MemLog{} }

func noWait(context.Context) error { return nil }

// Append implements Log.
func (m *MemLog) Append(rec Record) (WaitFunc, error) {
	if rec.Type == snapshotMark {
		return nil, ErrReservedType
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	m.recs = append(m.recs, Record{Type: rec.Type, Data: append([]byte(nil), rec.Data...)})
	m.size += recordSize(rec)
	return noWait, nil
}

// Replay implements Log.
func (m *MemLog) Replay(fn func(Record) error) error {
	m.mu.Lock()
	recs := append([]Record(nil), m.recs...)
	m.mu.Unlock()
	for _, r := range recs {
		if err := fn(r); err != nil {
			return err
		}
	}
	return nil
}

// Rotate implements Log; the marker is the number of records so far.
func (m *MemLog) Rotate() (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return 0, ErrClosed
	}
	return uint64(len(m.recs)), nil
}

// WriteSnapshot implements Log.
func (m *MemLog) WriteSnapshot(marker uint64, snap []Record) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	next := make([]Record, 0, len(snap)+len(m.recs)-int(marker))
	var size int64
	for _, r := range snap {
		next = append(next, Record{Type: r.Type, Data: append([]byte(nil), r.Data...)})
		size += recordSize(r)
	}
	for _, r := range m.recs[marker:] {
		next = append(next, r)
		size += recordSize(r)
	}
	m.recs, m.size = next, size
	return nil
}

// Size implements Log.
func (m *MemLog) Size() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.size
}

// Close implements Log.
func (m *MemLog) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}
