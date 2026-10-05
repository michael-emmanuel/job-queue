package replication

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

var bg = context.Background()

func openLog(t testing.TB, path string) *Log {
	t.Helper()
	l, err := OpenLog(path)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func tmpLog(t testing.TB) (*Log, string) {
	path := filepath.Join(t.TempDir(), "rlog")
	l := openLog(t, path)
	t.Cleanup(func() { l.Close() })
	return l, path
}

func put(t testing.TB, l *Log, epoch uint64, s string) uint64 {
	t.Helper()
	idx, err := l.Append(epoch, 1, []byte(s))
	if err != nil {
		t.Fatal(err)
	}
	if err := l.WaitDurable(bg, idx); err != nil {
		t.Fatal(err)
	}
	return idx
}

func dump(t testing.TB, l *Log) []string {
	t.Helper()
	var out []string
	if err := l.Replay(func(e Entry) error {
		out = append(out, fmt.Sprintf("%d@%d:%s", e.Index, e.Epoch, e.Data))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func eq(t testing.TB, got []string, want ...string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("log = %v, want %v", got, want)
	}
}

func TestAppendReadAndRecover(t *testing.T) {
	l, path := tmpLog(t)
	for i := 1; i <= 50; i++ {
		put(t, l, 1+uint64(i/20), fmt.Sprint("e", i))
	}
	if l.LastIndex() != 50 || l.LastEpoch() != 3 {
		t.Fatalf("last = %d@%d", l.LastIndex(), l.LastEpoch())
	}
	es, err := l.Read(10, 5, 1<<20)
	if err != nil || len(es) != 5 || es[0].Index != 10 || string(es[4].Data) != "e14" {
		t.Fatalf("read: %v %v", es, err)
	}
	if es, _ := l.Read(48, 100, 1<<20); len(es) != 3 {
		t.Fatalf("read tail: %d", len(es))
	}
	if es, _ := l.Read(51, 10, 1<<20); len(es) != 0 {
		t.Fatal("read past end")
	}
	if e, ok := l.EpochAt(25); !ok || e != 2 {
		t.Fatalf("EpochAt(25) = %d %v", e, ok)
	}
	if e, ok := l.EpochAt(0); !ok || e != 0 {
		t.Fatal("EpochAt(0)")
	}
	if _, ok := l.EpochAt(51); ok {
		t.Fatal("EpochAt beyond log")
	}
	l.Close()

	l2 := openLog(t, path)
	defer l2.Close()
	if l2.LastIndex() != 50 || l2.LastEpoch() != 3 || l2.DurableIndex() != 50 {
		t.Fatalf("recovered %d@%d durable %d", l2.LastIndex(), l2.LastEpoch(), l2.DurableIndex())
	}
	if got := dump(t, l2); len(got) != 50 || got[49] != "50@3:e50" {
		t.Fatalf("replay: %d entries", len(got))
	}
}

func TestReadRespectsByteLimitButAlwaysReturnsOne(t *testing.T) {
	l, _ := tmpLog(t)
	for i := 0; i < 10; i++ {
		put(t, l, 1, string(make([]byte, 1000)))
	}
	es, _ := l.Read(1, 100, 2500)
	if len(es) != 3 {
		t.Fatalf("got %d entries for a 2500-byte budget", len(es))
	}
	if es, _ := l.Read(1, 100, 1); len(es) != 1 {
		t.Fatalf("got %d entries for a tiny budget, want exactly 1", len(es))
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	l, path := tmpLog(t)
	for i := 1; i <= 5; i++ {
		put(t, l, 1, fmt.Sprint("e", i))
	}
	l.Close()
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.Write(encodeFrame(kindEntry, Entry{Index: 6, Epoch: 1, Data: []byte("torn")})[:15])
	f.Close()

	l2 := openLog(t, path)
	defer l2.Close()
	if l2.LastIndex() != 5 {
		t.Fatalf("last = %d", l2.LastIndex())
	}
	put(t, l2, 1, "after")
	eq(t, dump(t, l2), "1@1:e1", "2@1:e2", "3@1:e3", "4@1:e4", "5@1:e5", "6@1:after")
}

func TestIndexGapIsFatalNotTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "rlog")
	f, _ := os.Create(path)
	f.Write(encodeFrame(kindEntry, Entry{Index: 1, Epoch: 1}))
	f.Write(encodeFrame(kindEntry, Entry{Index: 3, Epoch: 1})) // valid frame, wrong index
	f.Close()
	if _, err := OpenLog(path); !errors.Is(err, ErrLogCorrupt) {
		t.Fatalf("err = %v, want ErrLogCorrupt", err)
	}
}

func TestPromiseSurvivesCrash(t *testing.T) {
	l, path := tmpLog(t)
	put(t, l, 1, "x")
	if err := l.Promise(7); err != nil {
		t.Fatal(err)
	}
	l.Abort() // crash right after the promise was acknowledged
	l2 := openLog(t, path)
	defer l2.Close()
	if l2.Promised() != 7 {
		t.Fatalf("promise forgotten: %d", l2.Promised())
	}
	if l2.LastIndex() != 1 {
		t.Fatalf("entries lost by promise: %d", l2.LastIndex())
	}
	if err := l2.Promise(3); err != nil || l2.Promised() != 7 {
		t.Fatalf("a lower promise must be a no-op: %d %v", l2.Promised(), err)
	}
}

func TestEntriesRaisePromisedEpoch(t *testing.T) {
	l, path := tmpLog(t)
	put(t, l, 4, "x")
	l.Close()
	l2 := openLog(t, path)
	defer l2.Close()
	if l2.Promised() != 4 {
		t.Fatalf("promised = %d", l2.Promised())
	}
}

func TestMergeSkipsDuplicatesAndNeverTruncatesMatching(t *testing.T) {
	l, _ := tmpLog(t)
	batch := []Entry{{Epoch: 1, Type: 1, Data: []byte("a")}, {Epoch: 1, Type: 1, Data: []byte("b")}, {Epoch: 1, Type: 1, Data: []byte("c")}}
	if last, err := l.Merge(0, batch); err != nil || last != 3 {
		t.Fatalf("%d %v", last, err)
	}
	// A delayed duplicate of an older, shorter message arrives later.
	if last, err := l.Merge(0, batch[:1]); err != nil || last != 1 {
		t.Fatalf("%d %v", last, err)
	}
	if l.LastIndex() != 3 {
		t.Fatalf("a stale duplicate truncated the log to %d", l.LastIndex())
	}
	eq(t, dump(t, l), "1@1:a", "2@1:b", "3@1:c")
}

func TestMergeTruncatesConflictingSuffixDurably(t *testing.T) {
	l, path := tmpLog(t)
	for _, s := range []string{"a", "b", "c", "d"} {
		put(t, l, 1, s)
	}
	// New leader (epoch 2) disagrees from index 3 on.
	last, err := l.Merge(2, []Entry{{Epoch: 2, Type: 1, Data: []byte("X")}})
	if err != nil || last != 3 {
		t.Fatalf("%d %v", last, err)
	}
	if err := l.WaitDurable(bg, 3); err != nil {
		t.Fatal(err)
	}
	eq(t, dump(t, l), "1@1:a", "2@1:b", "3@2:X")
	l.Abort()

	l2 := openLog(t, path)
	defer l2.Close()
	eq(t, dump(t, l2), "1@1:a", "2@1:b", "3@2:X") // the deleted entries must not come back
	if l2.LastEpoch() != 2 {
		t.Fatalf("last epoch %d", l2.LastEpoch())
	}
}

func TestGroupCommit(t *testing.T) {
	l, _ := tmpLog(t)
	var syncs int
	var mu sync.Mutex
	l.syncFn = func(f *os.File) error {
		mu.Lock()
		syncs++
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return f.Sync()
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			put(t, l, 1, "x")
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	if syncs == 0 || syncs >= 50 {
		t.Fatalf("%d appends took %d fsyncs", 100, syncs)
	}
	if l.LastIndex() != 100 || l.DurableIndex() != 100 {
		t.Fatalf("%d/%d", l.LastIndex(), l.DurableIndex())
	}
}

func TestOnDurableCallback(t *testing.T) {
	l, _ := tmpLog(t)
	got := make(chan struct{}, 8)
	l.SetOnDurable(func() { got <- struct{}{} })
	l.Append(1, 1, []byte("x"))
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("onDurable never fired")
	}
}

func TestFsyncFailureIsSticky(t *testing.T) {
	l, _ := tmpLog(t)
	boom := errors.New("disk on fire")
	l.syncFn = func(*os.File) error { return boom }
	idx, _ := l.Append(1, 1, []byte("x"))
	if err := l.WaitDurable(bg, idx); !errors.Is(err, boom) {
		t.Fatalf("wait: %v", err)
	}
	if _, err := l.Append(1, 1, []byte("y")); !errors.Is(err, boom) {
		t.Fatalf("append after failure: %v", err)
	}
	if err := l.Promise(9); !errors.Is(err, boom) {
		t.Fatalf("promise after failure: %v", err)
	}
}

func TestConcurrentAppendReadMerge(t *testing.T) {
	l, _ := tmpLog(t)
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				l.Append(1, 1, []byte("x"))
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			l.Read(1, 50, 1<<20)
			l.LastIndex()
		}
	}()
	wg.Wait()
	if l.LastIndex() != 800 {
		t.Fatalf("last = %d", l.LastIndex())
	}
	l.WaitDurable(bg, 800)
}
