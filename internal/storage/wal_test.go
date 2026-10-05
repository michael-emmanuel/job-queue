package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func rec(t byte, s string) Record { return Record{Type: t, Data: []byte(s)} }

func appendWait(t testing.TB, l Log, r Record) {
	t.Helper()
	wait, err := l.Append(r)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := wait(context.Background()); err != nil {
		t.Fatalf("wait: %v", err)
	}
}

func replayAll(t testing.TB, l Log) []Record {
	t.Helper()
	var out []Record
	if err := l.Replay(func(r Record) error {
		out = append(out, Record{Type: r.Type, Data: append([]byte(nil), r.Data...)})
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	return out
}

func mustOpen(t testing.TB, opts Options) *WAL {
	t.Helper()
	w, err := Open(opts)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return w
}

func lastSegment(t testing.TB, dir string) string {
	t.Helper()
	ids, err := listSegments(dir)
	if err != nil || len(ids) == 0 {
		t.Fatalf("list segments: %v %v", ids, err)
	}
	return segPath(dir, ids[len(ids)-1])
}

// Both implementations must satisfy the same contract.
func TestLogConformance(t *testing.T) {
	impls := map[string]func(t *testing.T) Log{
		"mem": func(t *testing.T) Log { return NewMemLog() },
		"wal": func(t *testing.T) Log { return mustOpen(t, Options{Dir: t.TempDir()}) },
	}
	for name, mk := range impls {
		t.Run(name, func(t *testing.T) {
			l := mk(t)
			defer l.Close()
			for i := 0; i < 5; i++ {
				appendWait(t, l, rec(1, fmt.Sprintf("a%d", i)))
			}
			marker, err := l.Rotate()
			if err != nil {
				t.Fatal(err)
			}
			appendWait(t, l, rec(2, "b0"))
			appendWait(t, l, rec(2, "b1"))
			before := l.Size()
			if err := l.WriteSnapshot(marker, []Record{rec(3, "s0"), rec(3, "s1")}); err != nil {
				t.Fatal(err)
			}
			got := replayAll(t, l)
			want := []string{"s0", "s1", "b0", "b1"}
			if len(got) != len(want) {
				t.Fatalf("got %d records, want %d", len(got), len(want))
			}
			for i, w := range want {
				if string(got[i].Data) != w {
					t.Errorf("record %d = %q, want %q", i, got[i].Data, w)
				}
			}
			if l.Size() >= before {
				t.Errorf("snapshot did not shrink the log: %d -> %d", before, l.Size())
			}
			if _, err := l.Append(Record{Type: snapshotMark}); !errors.Is(err, ErrReservedType) {
				t.Errorf("reserved type accepted: %v", err)
			}
		})
	}
}

func TestWALRoundTripAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir})
	for i := 0; i < 100; i++ {
		appendWait(t, w, rec(1, fmt.Sprintf("r%03d", i)))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	w2 := mustOpen(t, Options{Dir: dir})
	defer w2.Close()
	got := replayAll(t, w2)
	if len(got) != 100 {
		t.Fatalf("recovered %d records, want 100", len(got))
	}
	for i, r := range got {
		if want := fmt.Sprintf("r%03d", i); string(r.Data) != want {
			t.Fatalf("record %d = %q, want %q", i, r.Data, want)
		}
	}
}

func TestTornTailIsTruncatedAndLogStaysWritable(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir})
	for i := 0; i < 10; i++ {
		appendWait(t, w, rec(1, fmt.Sprintf("r%d", i)))
	}
	w.Close()

	// Simulate a crash mid-record: a header promising 100 bytes, 5 delivered.
	f, err := os.OpenFile(lastSegment(t, dir), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{100, 0, 0, 0, 1, 2, 3, 4, 9, 9, 9, 9, 9})
	f.Close()

	w2 := mustOpen(t, Options{Dir: dir})
	if w2.TruncatedBytes() != 13 {
		t.Errorf("TruncatedBytes = %d, want 13", w2.TruncatedBytes())
	}
	if got := replayAll(t, w2); len(got) != 10 {
		t.Fatalf("recovered %d, want 10", len(got))
	}
	appendWait(t, w2, rec(1, "after"))
	w2.Close()

	w3 := mustOpen(t, Options{Dir: dir})
	defer w3.Close()
	got := replayAll(t, w3)
	if len(got) != 11 || string(got[10].Data) != "after" {
		t.Fatalf("append after truncation lost: %d records", len(got))
	}
}

func TestChecksumMismatchInTailDropsOnlyThatRecord(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir})
	for i := 0; i < 5; i++ {
		appendWait(t, w, rec(1, "payload-payload"))
	}
	w.Close()
	path := lastSegment(t, dir)
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 0xFF // flip a bit in the final record's data
	os.WriteFile(path, b, 0o644)

	w2 := mustOpen(t, Options{Dir: dir})
	defer w2.Close()
	if got := replayAll(t, w2); len(got) != 4 {
		t.Fatalf("recovered %d, want 4", len(got))
	}
}

func TestCorruptionInSealedSegmentIsFatal(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir, SegmentBytes: 64})
	for i := 0; i < 20; i++ {
		appendWait(t, w, rec(1, "0123456789"))
	}
	w.Close()
	ids, _ := listSegments(dir)
	if len(ids) < 3 {
		t.Fatalf("expected several segments, got %d", len(ids))
	}
	first := segPath(dir, ids[0])
	b, _ := os.ReadFile(first)
	b[headerSize+2] ^= 0xFF
	os.WriteFile(first, b, 0o644)

	w2 := mustOpen(t, Options{Dir: dir, SegmentBytes: 64})
	defer w2.Close()
	err := w2.Replay(func(Record) error { return nil })
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Replay error = %v, want ErrCorrupt", err)
	}
}

func TestSegmentRotationPreservesOrder(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir, SegmentBytes: 128})
	for i := 0; i < 200; i++ {
		appendWait(t, w, rec(1, fmt.Sprintf("r%04d", i)))
	}
	w.Close()
	ids, _ := listSegments(dir)
	if len(ids) < 5 {
		t.Fatalf("expected rotation, got %d segments", len(ids))
	}
	w2 := mustOpen(t, Options{Dir: dir, SegmentBytes: 128})
	defer w2.Close()
	got := replayAll(t, w2)
	if len(got) != 200 {
		t.Fatalf("recovered %d, want 200", len(got))
	}
	for i, r := range got {
		if want := fmt.Sprintf("r%04d", i); string(r.Data) != want {
			t.Fatalf("record %d = %q, want %q", i, r.Data, want)
		}
	}
}

func TestSnapshotThenRestartDropsOldSegments(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir, SegmentBytes: 128})
	for i := 0; i < 100; i++ {
		appendWait(t, w, rec(1, fmt.Sprintf("old%03d", i)))
	}
	marker, err := w.Rotate()
	if err != nil {
		t.Fatal(err)
	}
	appendWait(t, w, rec(2, "new0"))
	if err := w.WriteSnapshot(marker, []Record{rec(3, "snap")}); err != nil {
		t.Fatal(err)
	}
	appendWait(t, w, rec(2, "new1"))
	w.Close()

	ids, _ := listSegments(dir)
	if ids[0] != marker {
		t.Errorf("oldest segment = %d, want snapshot segment %d", ids[0], marker)
	}
	w2 := mustOpen(t, Options{Dir: dir, SegmentBytes: 128})
	defer w2.Close()
	got := replayAll(t, w2)
	want := []string{"snap", "new0", "new1"}
	if len(got) != len(want) {
		t.Fatalf("got %d records, want %d", len(got), len(want))
	}
	for i, s := range want {
		if string(got[i].Data) != s {
			t.Errorf("record %d = %q, want %q", i, got[i].Data, s)
		}
	}
}

// A crash after the snapshot rename but before old segments are deleted must
// not resurrect the old records on top of the snapshot.
func TestCrashBetweenSnapshotRenameAndCleanup(t *testing.T) {
	dir := t.TempDir()
	stash := t.TempDir()
	w := mustOpen(t, Options{Dir: dir, SegmentBytes: 128})
	for i := 0; i < 60; i++ {
		appendWait(t, w, rec(1, fmt.Sprintf("old%03d", i)))
	}
	marker, _ := w.Rotate()
	ids, _ := listSegments(dir)
	for _, id := range ids { // save copies of everything older than the marker
		if id < marker {
			b, _ := os.ReadFile(segPath(dir, id))
			os.WriteFile(segPath(stash, id), b, 0o644)
		}
	}
	if err := w.WriteSnapshot(marker, []Record{rec(3, "snap")}); err != nil {
		t.Fatal(err)
	}
	w.Close()
	// "Crash" outcome: old segments are back on disk next to the snapshot.
	ents, _ := os.ReadDir(stash)
	for _, e := range ents {
		b, _ := os.ReadFile(filepath.Join(stash, e.Name()))
		os.WriteFile(filepath.Join(dir, e.Name()), b, 0o644)
	}

	w2 := mustOpen(t, Options{Dir: dir, SegmentBytes: 128})
	defer w2.Close()
	got := replayAll(t, w2)
	if len(got) != 1 || string(got[0].Data) != "snap" {
		t.Fatalf("stale segments leaked into replay: %d records", len(got))
	}
}

func TestUnfinishedSnapshotTempFileIsRemoved(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir})
	appendWait(t, w, rec(1, "x"))
	w.Close()
	junk := filepath.Join(dir, segPath("", 1)+".tmp")
	os.WriteFile(junk, []byte("partial snapshot"), 0o644)
	w2 := mustOpen(t, Options{Dir: dir})
	defer w2.Close()
	if _, err := os.Stat(junk); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temp file survived Open: %v", err)
	}
	if got := replayAll(t, w2); len(got) != 1 {
		t.Errorf("recovered %d records, want 1", len(got))
	}
}

func TestGroupCommitSharesFsyncs(t *testing.T) {
	w := mustOpen(t, Options{Dir: t.TempDir()})
	defer w.Close()
	w.syncFn = func(f *os.File) error { time.Sleep(5 * time.Millisecond); return f.Sync() }

	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			appendWait(t, w, rec(1, "x"))
		}()
	}
	wg.Wait()
	if s := w.Syncs(); s == 0 || s >= n/2 {
		t.Errorf("%d appends took %d fsyncs; group commit should need far fewer", n, s)
	}
}

func TestFsyncFailureIsStickyAndFailsWaiters(t *testing.T) {
	w := mustOpen(t, Options{Dir: t.TempDir()})
	defer w.Close()
	boom := errors.New("disk on fire")
	w.syncFn = func(*os.File) error { return boom }

	wait, err := w.Append(rec(1, "x"))
	if err != nil {
		t.Fatal(err)
	}
	if err := wait(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("wait error = %v, want %v", err, boom)
	}
	if _, err := w.Append(rec(1, "y")); !errors.Is(err, boom) {
		t.Fatalf("append after fsync failure = %v, want sticky %v", err, boom)
	}
}

func TestAbortKeepsEverythingAcknowledged(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir})
	for i := 0; i < 20; i++ {
		appendWait(t, w, rec(1, fmt.Sprintf("acked%d", i)))
	}
	w.Append(rec(1, "unacked")) // never waited on
	w.Abort()

	w2 := mustOpen(t, Options{Dir: dir})
	defer w2.Close()
	got := replayAll(t, w2)
	if len(got) < 20 || len(got) > 21 {
		t.Fatalf("recovered %d records, want 20 (or 21 if the unacked one raced to disk)", len(got))
	}
	for i := 0; i < 20; i++ {
		if want := fmt.Sprintf("acked%d", i); string(got[i].Data) != want {
			t.Fatalf("record %d = %q, want %q", i, got[i].Data, want)
		}
	}
}

func TestSyncNoneStillDurableOnClose(t *testing.T) {
	dir := t.TempDir()
	w := mustOpen(t, Options{Dir: dir, Sync: SyncNone})
	for i := 0; i < 10; i++ {
		appendWait(t, w, rec(1, "x"))
	}
	if w.Syncs() != 0 {
		t.Errorf("SyncNone performed %d fsyncs on the append path", w.Syncs())
	}
	w.Close()
	w2 := mustOpen(t, Options{Dir: dir, Sync: SyncNone})
	defer w2.Close()
	if got := replayAll(t, w2); len(got) != 10 {
		t.Fatalf("recovered %d, want 10", len(got))
	}
}

func TestPeriodicSyncEventuallySyncs(t *testing.T) {
	w := mustOpen(t, Options{Dir: t.TempDir(), Sync: SyncPeriodic, SyncEvery: 5 * time.Millisecond})
	defer w.Close()
	appendWait(t, w, rec(1, "x")) // returns immediately under SyncPeriodic
	deadline := time.Now().Add(2 * time.Second)
	for w.Syncs() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("periodic syncer never fsynced")
		}
		time.Sleep(time.Millisecond)
	}
}

func BenchmarkAppendSyncNone(b *testing.B) {
	w, err := Open(Options{Dir: b.TempDir(), Sync: SyncNone})
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	r := Record{Type: 1, Data: make([]byte, 256)}
	b.SetBytes(int64(recordSize(r)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(r); err != nil {
			b.Fatal(err)
		}
	}
}
