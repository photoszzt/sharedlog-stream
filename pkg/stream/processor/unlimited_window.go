package processor

import (
	"math"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"golang.org/x/xerrors"
)

var (
	win               = NewUnlimitedWindow(0)
	_                 = commtypes.Window(win)
	notAUnlimitWindow = xerrors.New("The window is not a UnlimitWindow")
)

type UnlimitedWindow struct {
	commtypes.BaseWindow
}

func NewUnlimitedWindow(startMs uint64) *UnlimitedWindow {
	return &UnlimitedWindow{
		commtypes.NewBaseWindow(startMs, math.MaxUint64),
	}
}

func (w *UnlimitedWindow) Overlap(other commtypes.Window) (bool, error) {
	_, ok := other.(*UnlimitedWindow)
	if !ok {
		return false, notAUnlimitWindow
	}
	return true, nil
}
