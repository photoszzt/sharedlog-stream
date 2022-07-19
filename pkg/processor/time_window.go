//go:generate msgp
//msgp:ignore TimeWindowJSONSerde TimeWindowMsgpSerde
package processor

import (
	"encoding/json"
	"fmt"
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

var _ = commtypes.Serde[TimeWindow](TimeWindowJSONSerde{})

func (s TimeWindowJSONSerde) Encode(value interface{}) ([]byte, error) {
	tw := value.(*TimeWindow)
	return json.Marshal(tw)
}

func (s TimeWindowJSONSerde) Decode(value []byte) (TimeWindow, error) {
	tw := TimeWindow{}
	if err := json.Unmarshal(value, &tw); err != nil {
		return nil, err
	}
	return tw, nil
}

type TimeWindowMsgpSerde struct{}

var _ = commtypes.Serde[TimeWindow](TimeWindowMsgpSerde{})

func (s TimeWindowMsgpSerde) Encode(value TimeWindow) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s TimeWindowMsgpSerde) Decode(value []byte) (TimeWindow, error) {
	tw := TimeWindow{}
	_, err := tw.UnmarshalMsg(value)
	if err != nil {
		return TimeWindow{}, err
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

func GetTimeWindowSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde[TimeWindow], error) {
	var twSerde commtypes.Serde[TimeWindow]
	if serdeFormat == commtypes.JSON {
		twSerde = TimeWindowJSONSerde{}
		return twSerde, nil
	} else if serdeFormat == commtypes.MSGP {
		twSerde = TimeWindowMsgpSerde{}
		return twSerde, nil
	} else {
		return nil, fmt.Errorf("unrecognized serde format: %v", serdeFormat)
	}
}
