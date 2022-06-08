package stats

import "time"

type ReportTimer struct {
	lastTs   time.Time
	duration time.Duration
}

func NewReportTimer(duration time.Duration) ReportTimer {
	return ReportTimer{
		duration: duration,
		lastTs:   time.Time{},
	}
}

func (r *ReportTimer) Check() bool {
	if r.lastTs.IsZero() {
		r.lastTs = time.Now()
		return false
	} else {
		return time.Since(r.lastTs) >= r.duration
	}
}

func (r *ReportTimer) Mark() time.Duration {
	duration := time.Since(r.lastTs)
	r.lastTs = time.Now()
	return duration
}
