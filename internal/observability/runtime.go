package observability

import "runtime"

// RuntimeCollector exposes Go runtime, process and disk metrics. dataDir is
// the directory whose filesystem utilisation is reported ("" to skip).
//
// runtime.ReadMemStats briefly stops the world; at a typical 15 second scrape
// interval that cost is negligible, but do not scrape it every few
// milliseconds.
func RuntimeCollector(dataDir string) Collector {
	return func(w *Writer) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		w.Gauge("go_goroutines", "Number of goroutines.", float64(runtime.NumGoroutine()))
		w.Gauge("go_memstats_alloc_bytes", "Bytes of allocated heap objects.", float64(m.Alloc))
		w.Gauge("go_memstats_heap_inuse_bytes", "Bytes in in-use heap spans.", float64(m.HeapInuse))
		w.Gauge("go_memstats_sys_bytes", "Bytes obtained from the OS.", float64(m.Sys))
		w.Counter("go_gc_cycles_total", "Completed GC cycles.", float64(m.NumGC))
		w.Counter("go_gc_pause_seconds_total", "Cumulative stop-the-world GC pause.", float64(m.PauseTotalNs)/1e9)
		if cpu, ok := cpuSeconds(); ok {
			w.Counter("process_cpu_seconds_total", "User plus system CPU time.", cpu)
		}
		if dataDir != "" {
			if total, free, ok := diskUsage(dataDir); ok {
				w.Gauge("data_disk_total_bytes", "Size of the filesystem holding the data directory.", float64(total))
				w.Gauge("data_disk_free_bytes", "Bytes available to unprivileged users on that filesystem.", float64(free))
			}
		}
	}
}
