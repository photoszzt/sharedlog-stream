//go:build stats
// +build stats

package stats

import (
	"fmt"
	"os"
	"sort"
)

func (c *ConcurrentInt64Collector) AddSample(sample int64) {
	c.mu.Lock()
	c.Int64Collector.AddSample(sample)
	c.mu.Unlock()
}

func (c *Int64Collector) AddSample(sample int64) {
	c.data = append(c.data, sample)
	if uint32(len(c.data)) >= c.min_report_samples && c.report_timer.Check() {
		il := Int64Slice(c.data)
		sort.Sort(il)
		p50 := P(il, 0.5)
		p90 := P(il, 0.9)
		p99 := P(il, 0.99)
		duration := c.report_timer.Mark()
		fmt.Fprintf(os.Stderr, "%s stats (%d samples): dur=%v, p50=%d, p90=%d, p99=%d\n",
			c.tag, len(c.data), duration, p50, p90, p99)
		c.data = make([]int64, 0)
	}
}

func (c *IntCollector) AddSample(sample int) {
	c.data = append(c.data, sample)
	if len(c.data) >= int(c.min_report_samples) && c.report_timer.Check() {
		il := IntSlice(c.data)
		sort.Sort(il)
		p50 := P(il, 0.5)
		p90 := P(il, 0.9)
		p99 := P(il, 0.99)
		duration := c.report_timer.Mark()
		fmt.Fprintf(os.Stderr, "%s stats (%d samples): dur=%v, p50=%d, p90=%d, p99=%d\n",
			c.tag, len(c.data), duration, p50, p90, p99)
		c.data = make([]int, 0)
	}
}

func (c *ConcurrentIntCollector) AddSample(sample int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.IntCollector.AddSample(sample)
}
