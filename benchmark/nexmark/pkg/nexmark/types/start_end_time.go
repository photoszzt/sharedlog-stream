//go:generate msgp
//msgp:ignore StartEndTimeJSONEncoder StartEndTimeJSONDecoder StartEndTimeJSONSerde
//msgp:ignore StartEndTimeMsgpEncoder StartEndTimeMsgpDecoder StartEndTimeMsgpSerde
package types

import (
	"encoding/json"

	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type StartEndTime struct {
	StartTime uint64 `json:"startTime" zid:"0" msg:"startTime"`
	EndTime   uint64 `json:"endTime" zid:"1" msg:"endTime"`
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
