package stats

import (
	"fmt"
	"os"
	"sharedlog-stream/pkg/utils/syncutils"
	"sort"
	"time"
)

const (
	DEFAULT_MIN_REPORT_SAMPLES = 200
	DEFAULT_COLLECT_DURATION   = time.Duration(10) * time.Second
)

func LatStart() time.Time {
	return time.Now()
}

type ConcurrentInt64Collector struct {
	mu syncutils.Mutex
	Int64Collector
}

func NewConcurrentInt64Collector(tag string, duration time.Duration) *ConcurrentInt64Collector {
	return &ConcurrentInt64Collector{
		Int64Collector: NewInt64Collector(tag, duration),
	}
}

func (c *ConcurrentInt64Collector) PrintRemainingStats() {
	c.Int64Collector.PrintRemainingStats()
}

type ConcurrentIntCollector struct {
	mu syncutils.Mutex
	IntCollector
}

func NewConcurrentIntCollector(tag string, duration time.Duration) *ConcurrentIntCollector {
	return &ConcurrentIntCollector{
		IntCollector: NewIntCollector(tag, duration),
	}
}

func (c *ConcurrentIntCollector) PrintRemainingStats() {
	c.IntCollector.PrintRemainingStats()
}

type Int64Collector struct {
	tag                string
	data               []int64
	report_timer       ReportTimer
	min_report_samples uint32
}

func NewInt64Collector(tag string, reportInterval time.Duration) Int64Collector {
	return Int64Collector{
		data:               make([]int64, 0, 128),
		report_timer:       NewReportTimer(reportInterval),
		tag:                tag,
		min_report_samples: DEFAULT_MIN_REPORT_SAMPLES,
	}
}

func (c *Int64Collector) PrintRemainingStats() {
	if len(c.data) > 0 {
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

type IntCollector struct {
	tag                string
	data               []int
	report_timer       ReportTimer
	min_report_samples uint32
}

func NewIntCollector(tag string, reportInterval time.Duration) IntCollector {
	return IntCollector{
		data:               make([]int, 0, 128),
		report_timer:       NewReportTimer(reportInterval),
		tag:                tag,
		min_report_samples: DEFAULT_MIN_REPORT_SAMPLES,
	}
}

func (c *IntCollector) PrintRemainingStats() {
	if len(c.data) > 0 {
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
