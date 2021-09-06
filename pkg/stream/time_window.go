package stream

import "golang.org/x/xerrors"

type TimeWindow struct {
	BaseWindow
}

var (
	w, _           = NewTimeWindow(0, 1)
	_              = Window(w)
	notATimeWindow = xerrors.New("The window is not a TimeWindow")
)

func NewTimeWindow(startMs uint64, endMs uint64) (*TimeWindow, error) {
	if startMs == endMs {
		return nil, windowEndNotLargerStart
	}
	return &TimeWindow{
		NewBaseWindow(startMs, endMs),
	}, nil
}

func (w *TimeWindow) Overlap(other Window) (bool, error) {
	_, ok := other.(*TimeWindow)
	if !ok {
		return false, notATimeWindow
	}
	return w.Start() < other.End() && other.Start() < w.End(), nil
}
