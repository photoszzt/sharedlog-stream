package stats

import (
	"fmt"
	"os"
	"sharedlog-stream/pkg/utils/syncutils"
	"time"

	"golang.org/x/exp/constraints"
	"golang.org/x/exp/slices"
)

const (
	DEFAULT_MIN_REPORT_SAMPLES = 200
	DEFAULT_COLLECT_DURATION   = time.Duration(10) * time.Second
)

func LatStart() time.Time {
	return time.Now()
}

type ConcurrentStatsCollector[E constraints.Ordered] struct {
	mu syncutils.Mutex
	StatsCollector[E]
}

func NewConcurrentStatsCollector[E constraints.Ordered](tag string, duration time.Duration) *ConcurrentStatsCollector[E] {
	return &ConcurrentStatsCollector[E]{
		StatsCollector: NewStatsCollector[E](tag, duration),
	}
}

func (c *ConcurrentStatsCollector[E]) PrintRemainingStats() {
	c.StatsCollector.PrintRemainingStats()
}

type StatsCollector[E constraints.Ordered] struct {
	tag                string
	data               []E
	report_timer       ReportTimer
	min_report_samples uint32
}

func NewStatsCollector[E constraints.Ordered](tag string, reportInterval time.Duration) StatsCollector[E] {
	return StatsCollector[E]{
		data:               make([]E, 0, 128),
		report_timer:       NewReportTimer(reportInterval),
		tag:                tag,
		min_report_samples: DEFAULT_MIN_REPORT_SAMPLES,
	}
}

func (c *StatsCollector[E]) PrintRemainingStats() {
	if len(c.data) > 0 {
		slices.Sort(c.data)
		p50 := POf(c.data, 0.5)
		p90 := POf(c.data, 0.9)
		p99 := POf(c.data, 0.99)
		duration := c.report_timer.Mark()
		fmt.Fprintf(os.Stderr, "%s stats (%d samples): dur=%v, p50=%v, p90=%v, p99=%v\n",
			c.tag, len(c.data), duration, p50, p90, p99)
		c.data = make([]E, 0)
	}
}
