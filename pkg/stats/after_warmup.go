package stats

import (
	"sync/atomic"
	"time"
)

type Warmup struct {
	initial     time.Time
	warmupTime  time.Duration
	afterWarmup bool
}

func NewWarmupChecker(warmupTime time.Duration) Warmup {
	afterWarmup := false
	if warmupTime == 0 {
		afterWarmup = true
	}
	return Warmup{
		warmupTime:  warmupTime,
		afterWarmup: afterWarmup,
	}
}

func (w *Warmup) Check() {
	if !w.afterWarmup && time.Since(w.initial) >= w.warmupTime {
		w.afterWarmup = true
	}
}

func (w *Warmup) AfterWarmup() bool {
	return w.afterWarmup
}

func (w *Warmup) StartWarmup() {
	w.initial = time.Now()
}

type WarmupGoroutineSafe struct {
	initial     time.Time
	warmupTime  time.Duration
	afterWarmup uint32
}

func NewWarmupGoroutineSafeChecker(warmupTime time.Duration) WarmupGoroutineSafe {
	afterWarmup := 0
	if warmupTime == 0 {
		afterWarmup = 1
	}
	return WarmupGoroutineSafe{
		warmupTime:  warmupTime,
		afterWarmup: uint32(afterWarmup),
	}
}

func (w *WarmupGoroutineSafe) Check() {
	if atomic.LoadUint32(&w.afterWarmup) == 0 && time.Since(w.initial) >= w.warmupTime {
		atomic.StoreUint32(&w.afterWarmup, 1)
	}
}

func (w *WarmupGoroutineSafe) AfterWarmup() bool {
	return atomic.LoadUint32(&w.afterWarmup) == 1
}

func (w *WarmupGoroutineSafe) StartWarmup() {
	w.initial = time.Now()
}
