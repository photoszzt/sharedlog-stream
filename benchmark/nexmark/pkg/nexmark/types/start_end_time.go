//go:generate msgp
//msgp:ignore StartEndTimeJSONEncoder StartEndTimeJSONDecoder StartEndTimeJSONSerde
//msgp:ignore StartEndTimeMsgpEncoder StartEndTimeMsgpDecoder StartEndTimeMsgpSerde
package types

import (
	"encoding/json"
	"fmt"

	"sharedlog-stream/pkg/commtypes"
)

type StartEndTime struct {
	StartTimeMs int64 `json:"sTs" msg:"sTs"`
	EndTimeMs   int64 `json:"eTs" msg:"eTs"`

	BaseInjTime `msg:"bInjT"`
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

func (se StartEndTime) String() string {
	return fmt.Sprintf("%d %d", se.StartTimeMs, se.EndTimeMs)
}

type StartEndTimeJSONEncoder struct{}

var _ = commtypes.Encoder(StartEndTimeJSONEncoder{})

func (e StartEndTimeJSONEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*StartEndTime)
	return json.Marshal(se)
}

type StartEndTimeJSONDecoder struct{}

var _ = commtypes.Decoder(StartEndTimeJSONDecoder{})

func (d StartEndTimeJSONDecoder) Decode(value []byte) (interface{}, error) {
	se := &StartEndTime{}
	err := json.Unmarshal(value, se)
	if err != nil {
		return nil, err
	}
	return se, nil
}

type StartEndTimeJSONSerde struct {
	StartEndTimeJSONEncoder
	StartEndTimeJSONDecoder
}

type StartEndTimeMsgpEncoder struct{}

var _ = commtypes.Encoder(StartEndTimeMsgpEncoder{})

func (e StartEndTimeMsgpEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*StartEndTime)
	return se.MarshalMsg(nil)
}

type StartEndTimeMsgpDecoder struct{}

var _ = commtypes.Decoder(StartEndTimeMsgpDecoder{})

func (d StartEndTimeMsgpDecoder) Decode(value []byte) (interface{}, error) {
	se := &StartEndTime{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return se, nil
}

type StartEndTimeMsgpSerde struct {
	StartEndTimeMsgpEncoder
	StartEndTimeMsgpDecoder
}

func GetStartEndTimeSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	var seSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		seSerde = StartEndTimeJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = StartEndTimeMsgpSerde{}
	} else {
		return nil, fmt.Errorf("unrecognized serde format: %v", serdeFormat)
	}
	return seSerde, nil
}
