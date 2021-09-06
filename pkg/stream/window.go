package stream

import (
	"time"

	"golang.org/x/xerrors"
)

var (
	windowEndNotLargerStart = xerrors.New("Window endMs should be greater than window startMs")
)

type Window interface {
	// returns window start in unix timestamp (ms)
	Start() uint64
	// returns window end in unix timestamp (ms)
	End() uint64
	// returns window start time
	StartTime() *time.Time
	// returns window end time
	EndTime() *time.Time
	// check if the given window overlaps with this window
	Overlap(other Window) (bool, error)
}

type BaseWindow struct {
	startTs uint64 // in ms
	endTs   uint64 // in ms
	startT  *time.Time
	endT    *time.Time
}

func NewBaseWindow(startTs uint64, endTs uint64) BaseWindow {
	startSecPart := startTs / 1000
	startNsPart := (startTs - startSecPart*1000) * 1000000
	startTime := time.Unix(int64(startSecPart), int64(startNsPart))
	endSecPart := endTs / 1000
	endNsPart := (endTs - endSecPart*1000) * 1000000
	endTime := time.Unix(int64(endSecPart), int64(endNsPart))
	return BaseWindow{
		startTs: startTs,
		endTs:   endTs,
		startT:  &startTime,
		endT:    &endTime,
	}
}

func (w *BaseWindow) Start() uint64 {
	return w.startTs
}

func (w *BaseWindow) End() uint64 {
	return w.endTs
}

func (w *BaseWindow) StartTime() *time.Time {
	return w.startT
}

func (w *BaseWindow) EndTime() *time.Time {
	return w.endT
}
