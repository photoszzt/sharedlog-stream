//go:build !stats
// +build !stats

package stats

func (c *ConcurrentStatsCollector[E]) AddSample(sample E) {}
func (c *StatsCollector[E]) AddSample(sample E)           {}
