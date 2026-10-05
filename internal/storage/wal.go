package storage

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// SyncPolicy controls when the WAL calls fsync.
type SyncPolicy int

const (
	// SyncAlways makes WaitFunc block until the record is fsynced. Concurrent
	// appenders share fsyncs (group commit). No acknowledged write is lost on
	// process or OS crash.
	SyncAlways SyncPolicy = iota
	// SyncPeriodic fsyncs on a timer; WaitFunc returns immediately. A crash
	// can lose up to SyncEvery of acknowledged writes.
	SyncPeriodic
	// SyncNone never fsyncs except on rotation and Close. The OS decides when
	// data reaches disk. Survives process crashes (data is in the page cache
	// once flushed) but not power loss.
	SyncNone
)

// Options configures a WAL.
type Options struct {
	Dir          string
	SegmentBytes int64         // rotate once the active segment reaches this size (default 64 MiB)
	Sync         SyncPolicy    // default SyncAlways
	SyncEvery    time.Duration // SyncPeriodic interval (default 50ms)
}

type segInfo struct {
	id   uint64
	size int64
}

// WAL is a segmented write-ahead log. Segment files are named by a
// monotonically increasing id and hold back-to-back framed records.
//
// Concurrency: one mutex guards the write buffer and sequence numbers; a
// single syncer goroutine performs fsync outside that mutex so appenders keep
// buffering while an fsync is in flight. That overlap is the group commit.
type WAL struct {
	opts   Options
	syncFn func(*os.File) error // overridable in tests

	mu     sync.Mutex
	f      *os.File
	bw     *bufio.Writer
	segs   []segInfo // ascending by id; last is active
	seq    uint64    // last assigned sequence number
	synced uint64    // highest sequence known durable
	failed error     // sticky: once set, every operation fails
	notify chan struct{}
	closed bool

	truncated int64 // bytes discarded from a torn tail during Open

	kick  chan struct{} // wakes the syncer (cap 1)
	done  chan struct{}
	wg    sync.WaitGroup
	syncs atomic.Uint64
}

var _ Log = (*WAL)(nil)

func segPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.seg", id))
}

func listSegments(dir string) ([]uint64, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint64
	for _, e := range ents {
		name := e.Name()
		if strings.HasSuffix(name, ".tmp") {
			// Leftover from a crash during snapshot writing; never renamed,
			// so it was never part of the log.
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
			continue
		}
		if !strings.HasSuffix(name, ".seg") {
			continue
		}
		id, err := strconv.ParseUint(strings.TrimSuffix(name, ".seg"), 10, 64)
		if err != nil {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func createSegment(dir string, id uint64) (*os.File, error) {
	f, err := os.OpenFile(segPath(dir, id), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	// The directory entry must be durable too, or the file can vanish after
	// a power loss even though its contents were fsynced.
	if err := syncDir(dir); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

// Open recovers the log in opts.Dir (creating it if needed):
//  1. delete unfinished snapshot temp files;
//  2. find the newest snapshot segment and delete everything older;
//  3. truncate a torn or corrupt tail off the newest segment.
//
// Call Replay to read the recovered records.
func Open(opts Options) (*WAL, error) {
	if opts.Dir == "" {
		return nil, errors.New("storage: Options.Dir is required")
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = 64 << 20
	}
	if opts.SyncEvery <= 0 {
		opts.SyncEvery = 50 * time.Millisecond
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	ids, err := listSegments(opts.Dir)
	if err != nil {
		return nil, err
	}
	base := 0
	for i := len(ids) - 1; i >= 0; i-- {
		snap, err := startsWithSnapshot(segPath(opts.Dir, ids[i]))
		if err != nil {
			return nil, err
		}
		if snap {
			base = i
			break
		}
	}
	for _, id := range ids[:base] {
		if err := os.Remove(segPath(opts.Dir, id)); err != nil {
			return nil, err
		}
	}
	ids = ids[base:]

	w := &WAL{
		opts:   opts,
		syncFn: (*os.File).Sync,
		notify: make(chan struct{}),
		kick:   make(chan struct{}, 1),
		done:   make(chan struct{}),
	}

	if len(ids) == 0 {
		f, err := createSegment(opts.Dir, 1)
		if err != nil {
			return nil, err
		}
		w.f = f
		w.segs = []segInfo{{id: 1}}
	} else {
		for _, id := range ids[:len(ids)-1] {
			st, err := os.Stat(segPath(opts.Dir, id))
			if err != nil {
				return nil, err
			}
			w.segs = append(w.segs, segInfo{id: id, size: st.Size()})
		}
		last := ids[len(ids)-1]
		path := segPath(opts.Dir, last)
		valid, err := scanSegment(path, nil)
		if err != nil && !errors.Is(err, errBadTail) {
			return nil, fmt.Errorf("storage: recovering %s: %w", path, err)
		}
		f, err := os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, err
		}
		if st.Size() > valid {
			// Torn tail from a crash mid-write (or corruption we cannot tell
			// apart from one). Everything after the last valid record was
			// never acknowledged under SyncAlways, so discarding is safe.
			w.truncated = st.Size() - valid
			if err := f.Truncate(valid); err != nil {
				f.Close()
				return nil, err
			}
			if err := f.Sync(); err != nil {
				f.Close()
				return nil, err
			}
		}
		if _, err := f.Seek(valid, 0); err != nil {
			f.Close()
			return nil, err
		}
		w.f = f
		w.segs = append(w.segs, segInfo{id: last, size: valid})
	}
	w.bw = bufio.NewWriterSize(w.f, 256<<10)

	if opts.Sync != SyncNone {
		w.wg.Add(1)
		go w.syncLoop()
	}
	return w, nil
}

// TruncatedBytes reports how many bytes of torn tail Open discarded.
func (w *WAL) TruncatedBytes() int64 { return w.truncated }

// Err returns the sticky failure that has disabled the log, if any.
func (w *WAL) Err() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failed
}

// Syncs returns the number of fsync calls made on the append path.
func (w *WAL) Syncs() uint64 { return w.syncs.Load() }

// Replay streams all live records to fn in order.
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	segs := append([]segInfo(nil), w.segs...)
	w.mu.Unlock()
	for _, s := range segs {
		path := segPath(w.opts.Dir, s.id)
		_, err := scanSegment(path, func(r Record) error {
			if r.Type == snapshotMark {
				return nil
			}
			return fn(r)
		})
		if errors.Is(err, errBadTail) {
			return fmt.Errorf("%w: segment %d", ErrCorrupt, s.id)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WAL) broadcastLocked() {
	close(w.notify)
	w.notify = make(chan struct{})
}

func (w *WAL) failLocked(err error) {
	if w.failed == nil {
		w.failed = fmt.Errorf("storage: wal failed: %w", err)
	}
	w.broadcastLocked()
}

// Append buffers rec and fixes its position in the log.
func (w *WAL) Append(rec Record) (WaitFunc, error) {
	if rec.Type == snapshotMark {
		return nil, ErrReservedType
	}
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil, ErrClosed
	}
	if w.failed != nil {
		err := w.failed
		w.mu.Unlock()
		return nil, err
	}
	if err := writeRecord(w.bw, rec); err != nil {
		// A partial buffered write corrupts the stream position; stop.
		w.failLocked(err)
		err = w.failed
		w.mu.Unlock()
		return nil, err
	}
	w.seq++
	seq := w.seq
	w.segs[len(w.segs)-1].size += recordSize(rec)
	if w.segs[len(w.segs)-1].size >= w.opts.SegmentBytes {
		// A failed rotation does not lose the record (it is buffered in the
		// current segment) and either fails the WAL or is retried on the next
		// append, so the error is intentionally not returned here.
		_ = w.rotateLocked()
	}
	w.mu.Unlock()

	switch w.opts.Sync {
	case SyncAlways:
		select {
		case w.kick <- struct{}{}:
		default:
		}
		return func(ctx context.Context) error { return w.waitDurable(ctx, seq) }, nil
	default:
		return func(context.Context) error { return nil }, nil
	}
}

func (w *WAL) waitDurable(ctx context.Context, seq uint64) error {
	for {
		w.mu.Lock()
		if w.synced >= seq {
			w.mu.Unlock()
			return nil
		}
		if w.failed != nil {
			err := w.failed
			w.mu.Unlock()
			return err
		}
		ch := w.notify
		w.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (w *WAL) syncLoop() {
	defer w.wg.Done()
	var tick <-chan time.Time
	if w.opts.Sync == SyncPeriodic {
		t := time.NewTicker(w.opts.SyncEvery)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-w.done:
			return
		case <-w.kick:
		case <-tick:
		}
		w.syncOnce()
	}
}

// syncOnce flushes the buffer under the mutex, then fsyncs without it.
func (w *WAL) syncOnce() {
	w.mu.Lock()
	if w.failed != nil || w.seq == w.synced {
		w.mu.Unlock()
		return
	}
	if err := w.bw.Flush(); err != nil {
		w.failLocked(err)
		w.mu.Unlock()
		return
	}
	target, f := w.seq, w.f
	w.mu.Unlock()

	err := w.syncFn(f)
	w.syncs.Add(1)

	w.mu.Lock()
	defer w.mu.Unlock()
	if err != nil && w.f == f {
		// Do not retry: after a failed fsync the kernel may have dropped the
		// dirty pages, so a later "successful" fsync proves nothing. Fail
		// the log and let the operator restart and recover from disk.
		w.failLocked(err)
		return
	}
	// If the segment rotated underneath us the rotation fsynced it (and
	// advanced synced past target), so a Sync error on the closed file is moot.
	if target > w.synced {
		w.synced = target
	}
	w.broadcastLocked()
}

// rotateLocked seals the active segment (flushed and fsynced) and opens the next.
func (w *WAL) rotateLocked() error {
	if w.failed != nil {
		return w.failed
	}
	if err := w.bw.Flush(); err != nil {
		w.failLocked(err)
		return w.failed
	}
	if err := w.syncFn(w.f); err != nil {
		w.failLocked(err)
		return w.failed
	}
	w.syncs.Add(1)
	next := w.segs[len(w.segs)-1].id + 1
	nf, err := createSegment(w.opts.Dir, next)
	if err != nil {
		return err // the old segment is intact and still active
	}
	old := w.f
	w.f = nf
	w.bw.Reset(nf)
	w.segs = append(w.segs, segInfo{id: next})
	w.synced = w.seq
	w.broadcastLocked()
	return old.Close()
}

// Rotate implements Log.
func (w *WAL) Rotate() (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrClosed
	}
	id := w.segs[len(w.segs)-1].id
	if err := w.rotateLocked(); err != nil {
		return 0, err
	}
	return id, nil
}

// WriteSnapshot implements Log. Crash safety:
//   - the snapshot is written to <id>.seg.tmp and fsynced before an atomic
//     rename over <id>.seg, so a crash leaves either the old segment or the
//     complete snapshot, never a partial one;
//   - the snapshot begins with snapshotMark, so if a crash happens after the
//     rename but before older segments are deleted, Open still starts
//     replay at the snapshot and removes the stale segments.
func (w *WAL) WriteSnapshot(marker uint64, recs []Record) error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return ErrClosed
	}
	found := false
	for _, s := range w.segs {
		if s.id == marker {
			found = true
		}
	}
	w.mu.Unlock()
	if !found {
		return fmt.Errorf("storage: snapshot marker %d is not a known segment", marker)
	}

	final := segPath(w.opts.Dir, marker)
	tmp := final + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	bw := bufio.NewWriterSize(f, 256<<10)
	size := recordSize(Record{Type: snapshotMark})
	werr := writeRecord(bw, Record{Type: snapshotMark})
	for i := 0; werr == nil && i < len(recs); i++ {
		werr = writeRecord(bw, recs[i])
		size += recordSize(recs[i])
	}
	if werr == nil {
		werr = bw.Flush()
	}
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		os.Remove(tmp)
		return werr
	}
	if err := os.Rename(tmp, final); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := syncDir(w.opts.Dir); err != nil {
		return err
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	kept := w.segs[:0:0]
	var rmErr error
	for _, s := range w.segs {
		switch {
		case s.id < marker:
			if err := os.Remove(segPath(w.opts.Dir, s.id)); err != nil && !errors.Is(err, os.ErrNotExist) {
				rmErr = err
			}
		case s.id == marker:
			kept = append(kept, segInfo{id: s.id, size: size})
		default:
			kept = append(kept, s)
		}
	}
	w.segs = kept
	return rmErr
}

// Size implements Log.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	var n int64
	for _, s := range w.segs {
		n += s.size
	}
	return n
}

// Close flushes and fsyncs everything, then releases the files.
func (w *WAL) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	close(w.done)
	w.mu.Unlock()
	w.wg.Wait()

	w.mu.Lock()
	defer w.mu.Unlock()
	var err error
	if w.failed == nil {
		err = w.bw.Flush()
		if err == nil {
			err = w.syncFn(w.f)
		}
		if err == nil {
			w.synced = w.seq
		} else {
			w.failLocked(err)
		}
	}
	w.broadcastLocked()
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Abort closes the files without flushing the write buffer, simulating a
// process kill. It exists for fault-injection tests: under SyncAlways every
// acknowledged record has already been flushed and fsynced, so a subsequent
// Open must recover all of them and nothing that was unacknowledged.
func (w *WAL) Abort() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.failed = ErrClosed
	close(w.done)
	w.f.Close()
	w.broadcastLocked()
	w.mu.Unlock()
	w.wg.Wait()
}
