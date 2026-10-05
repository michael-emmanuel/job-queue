// Package replication implements leader/follower replication of a
// partition's record log with quorum commit, epoch fencing and log
// reconciliation.
//
// It is Raft's log-replication half with the election half replaced by an
// external coordinator that assigns epochs (see internal/cluster). That
// substitution is the central simplification, and it moves the safety burden
// onto the coordinator; see docs/design-notes-m3.md for the exact argument
// and its limits.
package replication

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// Entry is one replicated log entry.
type Entry struct {
	Index uint64 `json:"index"`
	Epoch uint64 `json:"epoch"`
	Type  byte   `json:"type"`
	Data  []byte `json:"data,omitempty"`
}

// TypeNoop is the entry a new leader appends so that entries from earlier
// epochs become committed (a leader may only commit by counting replicas for
// entries of its own epoch).
const TypeNoop byte = 0xFC

const (
	kindEntry   byte = 1
	kindPromise byte = 2
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	errTorn = errors.New("replication: torn or corrupt frame")
	// ErrLogCorrupt means an entry sequence violation that cannot be a torn tail.
	ErrLogCorrupt = errors.New("replication: log corrupt")
	// ErrLogClosed is returned after Close.
	ErrLogClosed = errors.New("replication: log closed")
)

// frame: [len u32][crc32c u32][kind u8][epoch u64][index u64][type u8][data]
// len counts everything after the crc field. Promise frames use index 0.
const frameFixed = 1 + 8 + 8 + 1

func encodeFrame(kind byte, e Entry) []byte {
	body := make([]byte, frameFixed+len(e.Data))
	body[0] = kind
	binary.LittleEndian.PutUint64(body[1:], e.Epoch)
	binary.LittleEndian.PutUint64(body[9:], e.Index)
	body[17] = e.Type
	copy(body[frameFixed:], e.Data)
	out := make([]byte, 8+len(body))
	binary.LittleEndian.PutUint32(out[0:], uint32(len(body)))
	binary.LittleEndian.PutUint32(out[4:], crc32.Checksum(body, crcTable))
	copy(out[8:], body)
	return out
}

func readFrame(r *bufio.Reader) (kind byte, e Entry, n int64, err error) {
	var hdr [8]byte
	if _, err = io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.EOF) {
			return 0, e, 0, io.EOF
		}
		return 0, e, 0, errTorn
	}
	l := binary.LittleEndian.Uint32(hdr[0:])
	if l < frameFixed || l > 64<<20 {
		return 0, e, 0, errTorn
	}
	body := make([]byte, l)
	if _, err = io.ReadFull(r, body); err != nil {
		return 0, e, 0, errTorn
	}
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(hdr[4:]) {
		return 0, e, 0, errTorn
	}
	e = Entry{Epoch: binary.LittleEndian.Uint64(body[1:]), Index: binary.LittleEndian.Uint64(body[9:]), Type: body[17]}
	if l > frameFixed {
		e.Data = body[frameFixed:]
	}
	return body[0], e, int64(8 + l), nil
}

// Log is an index-addressable, truncatable, durable entry log with a
// persisted epoch promise. Indexes start at 1 and are contiguous.
//
// Why not reuse storage.WAL: replication needs random access by index (to
// serve a lagging follower) and suffix truncation (to discard entries from a
// deposed leader). The WAL is append-only and sequentially read. The group
// commit design is the same.
type Log struct {
	path   string
	syncFn func(*os.File) error // overridable in tests

	mu        sync.Mutex
	f         *os.File
	bw        *bufio.Writer
	offs      []int64  // offs[i] = file offset of the entry with index i+1
	epochs    []uint64 // epochs[i] = epoch of the entry with index i+1
	end       int64    // file size including buffered bytes
	promised  uint64
	durable   uint64 // highest index known fsynced
	gen       uint64 // bumped by truncation so a stale fsync cannot claim durability
	failed    error
	closed    bool
	notify    chan struct{}
	onDurable func()

	kick chan struct{}
	done chan struct{}
	wg   sync.WaitGroup
}

// OpenLog recovers (or creates) the log at path. A torn tail is truncated.
func OpenLog(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	l := &Log{path: path, f: f, syncFn: (*os.File).Sync, notify: make(chan struct{}),
		kick: make(chan struct{}, 1), done: make(chan struct{})}

	br := bufio.NewReaderSize(f, 256<<10)
	var off int64
	for {
		kind, e, n, err := readFrame(br)
		if errors.Is(err, io.EOF) {
			break
		}
		if errors.Is(err, errTorn) {
			if terr := f.Truncate(off); terr != nil {
				f.Close()
				return nil, terr
			}
			if serr := f.Sync(); serr != nil {
				f.Close()
				return nil, serr
			}
			break
		}
		if err != nil {
			f.Close()
			return nil, err
		}
		switch kind {
		case kindPromise:
			l.promised = max(l.promised, e.Epoch)
		case kindEntry:
			if e.Index != uint64(len(l.offs))+1 {
				f.Close()
				return nil, fmt.Errorf("%w: entry %d at position %d", ErrLogCorrupt, e.Index, len(l.offs)+1)
			}
			l.offs = append(l.offs, off)
			l.epochs = append(l.epochs, e.Epoch)
			l.promised = max(l.promised, e.Epoch)
		default:
			f.Close()
			return nil, fmt.Errorf("%w: unknown frame kind %d", ErrLogCorrupt, kind)
		}
		off += n
	}
	if _, err := f.Seek(off, io.SeekStart); err != nil {
		f.Close()
		return nil, err
	}
	l.end = off
	l.bw = bufio.NewWriterSize(f, 256<<10)
	l.durable = uint64(len(l.offs)) // whatever survived on disk is, by definition, durable
	l.wg.Add(1)
	go l.syncLoop()
	return l, nil
}

// SetOnDurable registers a callback invoked (without locks held) whenever
// the durable index advances. The replica uses it to recount the quorum.
func (l *Log) SetOnDurable(fn func()) {
	l.mu.Lock()
	l.onDurable = fn
	l.mu.Unlock()
}

// LastIndex returns the index of the last entry (0 if empty).
func (l *Log) LastIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return uint64(len(l.offs))
}

// LastEpoch returns the epoch of the last entry (0 if empty).
func (l *Log) LastEpoch() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.epochs) == 0 {
		return 0
	}
	return l.epochs[len(l.epochs)-1]
}

// EpochAt returns the epoch of entry i; index 0 has epoch 0. ok is false
// when i is beyond the log.
func (l *Log) EpochAt(i uint64) (epoch uint64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.epochAtLocked(i)
}

func (l *Log) epochAtLocked(i uint64) (uint64, bool) {
	if i == 0 {
		return 0, true
	}
	if i > uint64(len(l.epochs)) {
		return 0, false
	}
	return l.epochs[i-1], true
}

// FirstIndexOfEpoch returns the first index whose entry has epoch e, scanning
// backwards from i; used to give a follower's conflict hint in one step.
func (l *Log) firstIndexOfEpochLocked(i, e uint64) uint64 {
	for i > 1 && l.epochs[i-2] == e {
		i--
	}
	return i
}

// Promised returns the highest epoch this log has promised or seen.
func (l *Log) Promised() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.promised
}

// Size returns the log's byte size.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.end
}

// DurableIndex returns the highest fsynced index.
func (l *Log) DurableIndex() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.durable
}

// Err returns the sticky failure that disabled the log, if any.
func (l *Log) Err() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.failed
}

func (l *Log) failLocked(err error) {
	if l.failed == nil {
		l.failed = fmt.Errorf("replication: log failed: %w", err)
	}
	l.broadcastLocked()
}

func (l *Log) broadcastLocked() {
	close(l.notify)
	l.notify = make(chan struct{})
}

func (l *Log) usableLocked() error {
	if l.closed {
		return ErrLogClosed
	}
	return l.failed
}

// Promise durably records that this replica will not accept entries from any
// epoch below e. It does not return until the promise is fsynced: replying to
// a Prepare before that would let a crash forget a promise the coordinator is
// relying on.
func (l *Log) Promise(e uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usableLocked(); err != nil {
		return err
	}
	if e <= l.promised {
		return nil
	}
	frame := encodeFrame(kindPromise, Entry{Epoch: e})
	if _, err := l.bw.Write(frame); err != nil {
		l.failLocked(err)
		return l.failed
	}
	l.end += int64(len(frame))
	if err := l.bw.Flush(); err != nil {
		l.failLocked(err)
		return l.failed
	}
	if err := l.syncFn(l.f); err != nil {
		l.failLocked(err)
		return l.failed
	}
	l.promised = e
	l.durable = uint64(len(l.offs)) // the fsync covered every buffered entry too
	l.broadcastLocked()
	return nil
}

func (l *Log) appendLocked(e Entry) {
	frame := encodeFrame(kindEntry, e)
	l.offs = append(l.offs, l.end)
	l.epochs = append(l.epochs, e.Epoch)
	_, err := l.bw.Write(frame)
	if err != nil {
		l.failLocked(err)
	}
	l.end += int64(len(frame))
	l.promised = max(l.promised, e.Epoch)
}

// Append adds one entry with the next index. Durability is asynchronous: use
// WaitDurable.
func (l *Log) Append(epoch uint64, typ byte, data []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usableLocked(); err != nil {
		return 0, err
	}
	idx := uint64(len(l.offs)) + 1
	l.appendLocked(Entry{Index: idx, Epoch: epoch, Type: typ, Data: data})
	if l.failed != nil {
		return 0, l.failed
	}
	select {
	case l.kick <- struct{}{}:
	default:
	}
	return idx, nil
}

// Merge applies a follower-side append: entries are expected to start at
// prevIndex+1 and the caller has already verified that the entry at
// prevIndex matches the leader's. For each entry: if the log already holds
// an entry at that index with the same epoch it is skipped (so duplicated or
// reordered messages are harmless and never truncate anything); if the epoch
// differs, that entry and everything after it is deleted before appending.
// It returns the index of the last entry covered by the message.
func (l *Log) Merge(prevIndex uint64, entries []Entry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.usableLocked(); err != nil {
		return 0, err
	}
	last := prevIndex
	appended := false
	for i, e := range entries {
		idx := prevIndex + 1 + uint64(i)
		last = idx
		if idx <= uint64(len(l.offs)) {
			if l.epochs[idx-1] == e.Epoch {
				continue
			}
			if err := l.truncateLocked(idx - 1); err != nil {
				return 0, err
			}
		}
		l.appendLocked(Entry{Index: idx, Epoch: e.Epoch, Type: e.Type, Data: e.Data})
		appended = true
	}
	if l.failed != nil {
		return 0, l.failed
	}
	if appended {
		select {
		case l.kick <- struct{}{}:
		default:
		}
	}
	return last, nil
}

// truncateLocked deletes every entry after keep and makes the deletion
// durable before anything new is appended: otherwise a crash could resurrect
// entries the replica already told a leader it had discarded.
func (l *Log) truncateLocked(keep uint64) error {
	if keep >= uint64(len(l.offs)) {
		return nil
	}
	if err := l.bw.Flush(); err != nil {
		l.failLocked(err)
		return l.failed
	}
	cut := l.offs[keep]
	if err := l.f.Truncate(cut); err != nil {
		l.failLocked(err)
		return l.failed
	}
	if _, err := l.f.Seek(cut, io.SeekStart); err != nil {
		l.failLocked(err)
		return l.failed
	}
	l.bw.Reset(l.f)
	if err := l.syncFn(l.f); err != nil {
		l.failLocked(err)
		return l.failed
	}
	l.offs, l.epochs, l.end = l.offs[:keep], l.epochs[:keep], cut
	l.durable = min(l.durable, keep)
	l.gen++
	l.broadcastLocked()
	return nil
}

// Read returns up to max entries starting at index from, stopping early once
// maxBytes of data have been collected (at least one entry is always returned
// if available).
func (l *Log) Read(from uint64, maxEntries int, maxBytes int) ([]Entry, error) {
	l.mu.Lock()
	if err := l.usableLocked(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	if from == 0 || from > uint64(len(l.offs)) {
		l.mu.Unlock()
		return nil, nil
	}
	if err := l.bw.Flush(); err != nil {
		l.failLocked(err)
		err = l.failed
		l.mu.Unlock()
		return nil, err
	}
	start, end := l.offs[from-1], l.end
	l.mu.Unlock()

	br := bufio.NewReaderSize(io.NewSectionReader(l.f, start, end-start), 64<<10)
	var out []Entry
	size := 0
	for len(out) < maxEntries {
		kind, e, _, err := readFrame(br)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if kind != kindEntry {
			continue
		}
		out = append(out, e)
		size += len(e.Data)
		if size >= maxBytes {
			break
		}
	}
	return out, nil
}

// Replay calls fn for every entry in order.
func (l *Log) Replay(fn func(Entry) error) error {
	l.mu.Lock()
	if err := l.usableLocked(); err != nil {
		l.mu.Unlock()
		return err
	}
	if err := l.bw.Flush(); err != nil {
		l.failLocked(err)
		err = l.failed
		l.mu.Unlock()
		return err
	}
	end := l.end
	l.mu.Unlock()
	br := bufio.NewReaderSize(io.NewSectionReader(l.f, 0, end), 256<<10)
	for {
		kind, e, _, err := readFrame(br)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if kind != kindEntry {
			continue
		}
		if err := fn(e); err != nil {
			return err
		}
	}
}

// WaitDurable blocks until entry idx is fsynced.
func (l *Log) WaitDurable(ctx context.Context, idx uint64) error {
	for {
		l.mu.Lock()
		if l.durable >= idx {
			l.mu.Unlock()
			return nil
		}
		if err := l.usableLocked(); err != nil {
			l.mu.Unlock()
			return err
		}
		if idx > uint64(len(l.offs)) {
			l.mu.Unlock()
			return fmt.Errorf("replication: waiting for index %d beyond the log (truncated?)", idx)
		}
		ch := l.notify
		l.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (l *Log) syncLoop() {
	defer l.wg.Done()
	for {
		select {
		case <-l.done:
			return
		case <-l.kick:
		}
		l.mu.Lock()
		if l.failed != nil || l.durable >= uint64(len(l.offs)) {
			l.mu.Unlock()
			continue
		}
		if err := l.bw.Flush(); err != nil {
			l.failLocked(err)
			l.mu.Unlock()
			continue
		}
		target, gen, f := uint64(len(l.offs)), l.gen, l.f
		l.mu.Unlock()

		err := l.syncFn(f)

		l.mu.Lock()
		cb := l.onDurable
		switch {
		case err != nil:
			l.failLocked(err) // never retry a failed fsync; see storage.WAL
		case gen == l.gen && target > l.durable && target <= uint64(len(l.offs)):
			l.durable = target
			l.broadcastLocked()
		}
		l.mu.Unlock()
		if err == nil && cb != nil {
			cb()
		}
	}
}

// Close flushes, fsyncs and releases the file.
func (l *Log) Close() error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	close(l.done)
	l.mu.Unlock()
	l.wg.Wait()
	l.mu.Lock()
	defer l.mu.Unlock()
	var err error
	if l.failed == nil {
		if err = l.bw.Flush(); err == nil {
			err = l.syncFn(l.f)
		}
	}
	l.broadcastLocked()
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Abort closes the file without flushing, simulating a process kill.
func (l *Log) Abort() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	close(l.done)
	l.f.Close()
	l.broadcastLocked()
	l.mu.Unlock()
	l.wg.Wait()
}
