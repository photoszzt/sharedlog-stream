//go:generate msgp
//msgp:ignore StartEndTimeJSONEncoder StartEndTimeJSONDecoder StartEndTimeJSONSerde
//msgp:ignore StartEndTimeMsgpEncoder StartEndTimeMsgpDecoder StartEndTimeMsgpSerde
package types

import (
	"encoding/json"
	"fmt"

	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type StartEndTime struct {
	StartTimeMs int64 `json:"sTs" msg:"sTs"`
	EndTimeMs   int64 `json:"eTs" msg:"eTs"`
}

var _ = fmt.Stringer(StartEndTime{})

func (se StartEndTime) Stringer() string {
	return fmt.Sprintf("StartEndTime: {StartTsMs: %d, EndTsMs: %d}", se.StartTimeMs, se.EndTimeMs)
}

func CompareStartEndTime(a, b *StartEndTime) int {
	if a.StartTimeMs < b.StartTimeMs {
		return -1
	} else { // a.st >= b.st
		if a.StartTimeMs == b.StartTimeMs {
			if a.EndTimeMs < b.EndTimeMs {
				return -1
			} else if a.EndTimeMs == b.EndTimeMs {
				return 0
			} else {
				return 1
			}
		} else {
			return 1
		}
	}
}

var _ = commtypes.EventTimeExtractor(&StartEndTime{})

func (se *StartEndTime) ExtractEventTime() (int64, error) {
	return se.StartTimeMs, nil
}

func (se StartEndTime) String() string {
	return fmt.Sprintf("%d %d", se.StartTimeMs, se.EndTimeMs)
}

type StartEndTimeJSONSerde struct{}

var _ = commtypes.Serde(StartEndTimeJSONSerde{})

func (e StartEndTimeJSONSerde) Encode(value interface{}) ([]byte, error) {
	se, ok := value.(*StartEndTime)
	if !ok {
		seTmp := value.(StartEndTime)
		se = &seTmp
	}
	return json.Marshal(se)
}

func (d StartEndTimeJSONSerde) Decode(value []byte) (interface{}, error) {
	se := StartEndTime{}
	err := json.Unmarshal(value, &se)
	if err != nil {
		return nil, err
	}
	return se, nil
}

type StartEndTimeMsgpSerde struct{}

var _ = commtypes.Serde(StartEndTimeMsgpSerde{})

func (e StartEndTimeMsgpSerde) Encode(value interface{}) ([]byte, error) {
	se, ok := value.(*StartEndTime)
	if !ok {
		seTmp := value.(StartEndTime)
		se = &seTmp
	}
	return se.MarshalMsg(nil)
}

func (d StartEndTimeMsgpSerde) Decode(value []byte) (interface{}, error) {
	se := StartEndTime{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return se, nil
}

func GetStartEndTimeSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	var seSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		seSerde = StartEndTimeJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = StartEndTimeMsgpSerde{}
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
	return seSerde, nil
}
