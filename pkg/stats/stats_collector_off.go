//go:build !stats
// +build !stats

package stats

func (c *ConcurrentInt64Collector) AddSample(sample int64) {}
func (c *ConcurrentIntCollector) AddSample(sample int64)   {}
func (c *Int64Collector) AddSample(sample int64)           {}
func (c *IntCollector) AddSample(sample int)               {}
