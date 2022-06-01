//go:generate msgp
//msgp:ignore TimeWindowJSONSerde TimeWindowMsgpSerde
package processor

import (
	"encoding/json"
	"math"
	"sharedlog-stream/pkg/commtypes"

	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
)

type TimeWindow struct {
	commtypes.BaseWindow
}

var (
	_              = commtypes.Window(&TimeWindow{})
	notATimeWindow = xerrors.New("The window is not a TimeWindow")
)

func NewTimeWindow(startMs int64, endMs int64) (*TimeWindow, error) {
	if startMs == endMs {
		return nil, commtypes.WindowEndNotLargerStart
	}
	return &TimeWindow{
		commtypes.NewBaseWindow(startMs, endMs),
	}, nil
}

func (w *TimeWindow) Overlap(other commtypes.Window) (bool, error) {
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

func TimeWindowForSize(startMs int64, windowSize int64) (*TimeWindow, error) {
	endMs := startMs + windowSize
	if endMs < 0 {
		log.Warn().Msg("window end time was truncated to int64 max")
		endMs = math.MaxInt64
	}
	return NewTimeWindow(startMs, endMs)
}
