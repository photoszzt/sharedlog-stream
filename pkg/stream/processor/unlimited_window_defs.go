package processor

import (
	"math"
	"sharedlog-stream/pkg/stream/processor/commtypes"
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
	startMs int64
}

func NewUnlimitedWindows(startMs int64) *UnlimitedWindows {
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
	startMs := start.Unix() * 1000
	if startMs < 0 {
		log.Fatal().Msg("start time (ms) cannot be negative")
	}
	return &UnlimitedWindows{
		startMs: startMs,
	}
}

func (w *UnlimitedWindows) MaxSize() int64 {
	return math.MaxInt64
}

func (w *UnlimitedWindows) GracePeriodMs() int64 {
	return 0
}

func (w *UnlimitedWindows) WindowsFor(timestamp int64) (map[int64]commtypes.Window, error) {
	windows := make(map[int64]commtypes.Window)
	if timestamp >= w.startMs {
		windows[w.startMs] = NewUnlimitedWindow(w.startMs)
	}
	return windows, nil
}
