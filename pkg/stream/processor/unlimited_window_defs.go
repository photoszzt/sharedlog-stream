package processor

import (
	"math"
	"time"

	"github.com/rs/zerolog/log"
)

const (
	DEFAULT_START_TIMESTAMP_MS = 0
)

var (
	a = NewDefaultUnlimitedWindows()
	_ = EnumerableWindowDefinition(a)
)

type UnlimitedWindows struct {
	startMs uint64
}

func NewUnlimitedWindows(startMs uint64) *UnlimitedWindows {
	return &UnlimitedWindows{
		startMs: startMs,
	}
}

func NewDefaultUnlimitedWindows() *UnlimitedWindows {
	return &UnlimitedWindows{
		startMs: DEFAULT_START_TIMESTAMP_MS,
	}
}

func NewUnlimitedWindowsStartOn(start time.Time) *UnlimitedWindows {
	startMs := start.UnixMilli()
	if startMs < 0 {
		log.Fatal().Msg("start time (ms) cannot be negative")
	}
	return &UnlimitedWindows{
		startMs: uint64(startMs),
	}
}

func (w *UnlimitedWindows) MaxSize() uint64 {
	return math.MaxUint64
}

func (w *UnlimitedWindows) GracePeriodMs() uint64 {
	return 0
}

func (w *UnlimitedWindows) WindowsFor(timestamp uint64) (map[uint64]Window, error) {
	windows := make(map[uint64]Window)
	if timestamp >= w.startMs {
		windows[w.startMs] = NewUnlimitedWindow(w.startMs)
	}
	return windows, nil
}
