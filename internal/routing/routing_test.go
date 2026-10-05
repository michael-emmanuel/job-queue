package routing

import (
	"fmt"
	"testing"
)

func TestDeterministicAndInRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		k := fmt.Sprint("key", i)
		a, b := Partition("email", k, 8), Partition("email", k, 8)
		if a != b || a < 0 || a >= 8 {
			t.Fatalf("key %s -> %d, %d", k, a, b)
		}
	}
	if Partition("q", "k", 1) != 0 || Partition("q", "k", 0) != 0 {
		t.Fatal("degenerate partition counts must map to 0")
	}
}

func TestQueueAndKeyBoundaryIsUnambiguous(t *testing.T) {
	same := 0
	for i := 0; i < 200; i++ {
		if Partition("ab", fmt.Sprint("c", i), 64) == Partition("a", fmt.Sprint("bc", i), 64) {
			same++
		}
	}
	if same > 20 { // expected ~200/64
		t.Fatalf("queue/key concatenation collides: %d/200", same)
	}
}

func TestDistinctKeysSpreadEvenly(t *testing.T) {
	const n, keys = 8, 80000
	counts := make([]int, n)
	for i := 0; i < keys; i++ {
		counts[Partition("payments", fmt.Sprintf("merchant-%d", i), n)]++
	}
	want := keys / n
	for p, c := range counts {
		if c < want*95/100 || c > want*105/100 {
			t.Errorf("partition %d holds %d keys, want about %d", p, c, want)
		}
	}
}

// Hashing balances keys, not load. This is the hot-partition problem.
func TestHotKeySkewIsNotFixedByHashing(t *testing.T) {
	const n, jobs = 8, 100000
	counts := make([]int, n)
	for i := 0; i < jobs; i++ {
		key := "merchant-hot" // one merchant sends 80% of traffic
		if i%5 == 0 {
			key = fmt.Sprintf("merchant-%d", i)
		}
		counts[Partition("payments", key, n)]++
	}
	hot := Partition("payments", "merchant-hot", n)
	if counts[hot] < jobs*80/100 {
		t.Fatalf("hot partition got %d of %d jobs", counts[hot], jobs)
	}
	t.Logf("hot partition %d received %.1f%% of load; the other %d partitions share the rest",
		hot, 100*float64(counts[hot])/jobs, n-1)
}

func BenchmarkPartition(b *testing.B) {
	for i := 0; i < b.N; i++ {
		Partition("email", "user-12345", 16)
	}
}
