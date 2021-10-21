//go:generate msgp
//msgp:ignore TimeWindowJSONSerde TimeWindowMsgpSerde
package processor

import (
	"encoding/json"

	"golang.org/x/xerrors"
)

type TimeWindow struct {
	BaseWindow
}

var (
	tw, _          = NewTimeWindow(0, 1)
	_              = Window(tw)
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

type TimeWindowJSONSerde struct{}

func (s TimeWindowJSONSerde) Encode(value interface{}) ([]byte, error) {
	tw := value.(*TimeWindow)
	return json.Marshal(tw)
}

func (s TimeWindowJSONSerde) Decode(value []byte) (interface{}, error) {
	tw := TimeWindow{}
	if err := json.Unmarshal(value, &tw); err != nil {
		return nil, err
	}
	return tw, nil
}

type TimeWindowMsgpSerde struct{}

func (s TimeWindowMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tw := value.(*TimeWindow)
	return tw.MarshalMsg(nil)
}

func (s TimeWindowMsgpSerde) Decode(value []byte) (interface{}, error) {
	tw := TimeWindow{}
	_, err := tw.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return tw, nil
}
