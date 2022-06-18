package stats

import (
	"sync"
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
	mu sync.Mutex
	Int64Collector
}

func NewConcurrentInt64Collector(tag string, duration time.Duration) ConcurrentInt64Collector {
	return ConcurrentInt64Collector{
		Int64Collector: NewInt64Collector(tag, duration),
	}
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
