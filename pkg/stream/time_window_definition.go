package stream

import (
	"time"

	"github.com/rs/zerolog/log"

	"golang.org/x/xerrors"
)

const (
	DEFAULT_RETENTION_MS = uint64(24 * 60 * 60 * 1000)
)

var (
	windowSizeLeqZero            = xerrors.New("Window size must be larger than zero")
	windowAdvanceLargerThanSize  = xerrors.New("window advance interval should be less than  window duration")
	windowAdvanceSmallerThanZero = xerrors.New("window advance interval should be larger than zero")
)

type TimeWindowDefinition struct {
	// size of the windows in ms
	SizeMs uint64
	// size of the window's advance interval in ms, i.e., by how much a window moves forward relative to
	// the previous one.
	AdvanceMs uint64
	graceMs   uint64
}

var (
	a = NewTimeWindowDefinitionWithSize(time.Duration(5) * time.Millisecond)
	_ = EnumerableWindowDefinition(a)
)

func NewTimeWindowDefinition(sizeMs uint64, advanceMs uint64, graceMs uint64) (*TimeWindowDefinition, error) {
	if sizeMs == 0 {
		return nil, windowSizeLeqZero
	}
	return &TimeWindowDefinition{
		SizeMs:    sizeMs,
		AdvanceMs: advanceMs,
		graceMs:   graceMs,
	}, nil
}

//
// Return a window definition with the given window size, and with the advance interval being equal to the window
// size.
// The time interval represented by the N-th window is: {@code [N * size, N * size + size)}.
//
// This provides the semantics of tumbling windows, which are fixed-sized, gap-less, non-overlapping windows.
// Tumbling windows are a special case of hopping windows with {@code advance == size}.
//
// @param size The size of the window
// @return a new window definition with default maintain duration of 1 day
// @return error if the specified window size is zero or negative
//
func NewTimeWindowDefinitionWithSize(size time.Duration) *TimeWindowDefinition {
	sizeMs := size.Milliseconds()
	if sizeMs <= 0 {
		log.Fatal().Err(windowSizeLeqZero)
	}
	return &TimeWindowDefinition{
		SizeMs:    uint64(sizeMs),
		AdvanceMs: uint64(sizeMs),
		graceMs:   DEFAULT_RETENTION_MS,
	}
}

//
// Set the advance ("hop") the window by the given interval, which
// specifies by how much a window moves forward relative to the previous one.
// The time interval represented by the N-th window is: {@code [N * advance, N * advance + size)}.
//
// This provides the semantics of hopping windows, which are fixed-sized, overlapping windows.
//
// @param advance The advance interval ("hop") of the window, with the requirement that {@code 0 < advance.toMillis() <= sizeMs}.
// @return error if the advance interval is negative, zero, or larger than the window size
//
func (w *TimeWindowDefinition) AdvanceBy(advance time.Duration) *TimeWindowDefinition {
	advanceMs := advance.Milliseconds()
	if advanceMs <= 0 {
		log.Fatal().Err(windowAdvanceSmallerThanZero)
	}
	if advanceMs > int64(w.SizeMs) {
		log.Fatal().Err(windowAdvanceLargerThanSize)
	}
	return &TimeWindowDefinition{
		SizeMs:    w.SizeMs,
		AdvanceMs: uint64(advanceMs),
		graceMs:   w.graceMs,
	}

}

func MaxUint64(a, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func (w *TimeWindowDefinition) WindowsFor(timestamp uint64) (map[uint64]Window, error) {
	windowStart := MaxUint64(0, timestamp-w.SizeMs+w.AdvanceMs) / w.AdvanceMs * w.AdvanceMs
	windows := make(map[uint64]Window)
	for windowStart <= timestamp {
		window, err := NewTimeWindow(windowStart, windowStart+w.SizeMs)
		if err != nil {
			return nil, err
		}
		windows[windowStart] = window
		windowStart += w.AdvanceMs
	}
	return windows, nil
}

func (w *TimeWindowDefinition) MaxSize() uint64 {
	return w.SizeMs
}

func (w *TimeWindowDefinition) GracePeriodMs() uint64 {
	return w.graceMs
}
