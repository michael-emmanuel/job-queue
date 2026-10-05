// Package routing maps a (queue, key) pair to a partition.
package routing

// Partition returns the partition in [0, n) that owns key within queue.
//
// Strategy: 64-bit FNV-1a of queue || 0x00 || key, modulo n. It is
// deterministic, needs no coordination or lookup table, and spreads
// distinct keys uniformly.
//
// Limits, deliberately accepted for now:
//   - Changing n remaps almost every key (about (n-1)/n of them), so the
//     partition count of a queue is fixed at creation.
//   - It balances KEYS, not LOAD. One key carrying most of the traffic
//     makes one hot partition no matter how well hashing spreads the rest.
//
// Evolution path: hash into a much larger fixed set of virtual shards
// (for example 1024) and keep a shard-to-partition table. Rebalancing then
// moves whole shards instead of rehashing keys, and consistent hashing
// becomes useful once nodes join and leave. See docs/design-notes-m2.md.
func Partition(queue, key string, n int) int {
	if n <= 1 {
		return 0
	}
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(queue); i++ {
		h = (h ^ uint64(queue[i])) * prime
	}
	h = (h ^ 0) * prime // separator so ("ab","c") and ("a","bc") differ
	for i := 0; i < len(key); i++ {
		h = (h ^ uint64(key[i])) * prime
	}
	return int(h % uint64(n))
}
