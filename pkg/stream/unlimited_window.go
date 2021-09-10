package stream

import (
	"math"

	"golang.org/x/xerrors"
)

var (
	win               = NewUnlimitedWindow(0)
	_                 = Window(win)
	notAUnlimitWindow = xerrors.New("The window is not a UnlimitWindow")
)

type UnlimitedWindow struct {
	BaseWindow
}

func NewUnlimitedWindow(startMs uint64) *UnlimitedWindow {
	return &UnlimitedWindow{
		NewBaseWindow(startMs, math.MaxUint64),
	}
}

func (w *UnlimitedWindow) Overlap(other Window) (bool, error) {
	_, ok := other.(*UnlimitedWindow)
	if !ok {
		return false, notAUnlimitWindow
	}
	return true, nil
}
