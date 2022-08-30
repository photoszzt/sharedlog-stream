//go:generate msgp
//msgp:ignore TimeWindowJSONSerde TimeWindowMsgpSerde
package commtypes

import (
	"encoding/json"
	"fmt"
	"math"

	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
)

type TimeWindow struct {
	BaseWindow
}

var (
	_              = Window(&TimeWindow{})
	notATimeWindow = xerrors.New("The window is not a TimeWindow")
)

func NewTimeWindow(startMs int64, endMs int64) (*TimeWindow, error) {
	if startMs == endMs {
		return nil, WindowEndNotLargerStart
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

func TimeWindowForSize(startMs int64, windowSize int64) (*TimeWindow, error) {
	endMs := startMs + windowSize
	if endMs < 0 {
		log.Warn().Msg("window end time was truncated to int64 max")
		endMs = math.MaxInt64
	}
	return NewTimeWindow(startMs, endMs)
}

func GetTimeWindowSerde(serdeFormat SerdeFormat) (Serde, error) {
	var twSerde Serde
	if serdeFormat == JSON {
		twSerde = TimeWindowJSONSerde{}
		return twSerde, nil
	} else if serdeFormat == MSGP {
		twSerde = TimeWindowMsgpSerde{}
		return twSerde, nil
	} else {
		return nil, fmt.Errorf("unrecognized serde format: %v", serdeFormat)
	}
}
