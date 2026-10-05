//go:build !unix

package observability

func cpuSeconds() (float64, bool)                    { return 0, false }
func diskUsage(string) (total, free uint64, ok bool) { return 0, 0, false }
