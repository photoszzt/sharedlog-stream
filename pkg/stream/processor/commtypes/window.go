//go:generate msgp
//msgp:ignore Window
package commtypes

import (
	"time"

	"golang.org/x/xerrors"
)

var (
	WindowEndNotLargerStart = xerrors.New("Window endMs should be greater than window startMs")
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
	StartTs uint64     `json:"startTs" msg:"startTs"` // in ms
	EndTs   uint64     `json:"endTs" msg:"endTs"`     // in ms
	startT  *time.Time `json:"-" msg:"-"`
	endT    *time.Time `json:"-" msg:"-"`
}

func NewBaseWindow(startTs uint64, endTs uint64) BaseWindow {
	startSecPart := startTs / 1000
	startNsPart := (startTs - startSecPart*1000) * 1000000
	startTime := time.Unix(int64(startSecPart), int64(startNsPart))
	endSecPart := endTs / 1000
	endNsPart := (endTs - endSecPart*1000) * 1000000
	endTime := time.Unix(int64(endSecPart), int64(endNsPart))
	return BaseWindow{
		StartTs: startTs,
		EndTs:   endTs,
		startT:  &startTime,
		endT:    &endTime,
	}
}

func (w *BaseWindow) Start() uint64 {
	return w.StartTs
}

func (w *BaseWindow) End() uint64 {
	return w.EndTs
}

func (w *BaseWindow) StartTime() *time.Time {
	return w.startT
}

func (w *BaseWindow) EndTime() *time.Time {
	return w.endT
}