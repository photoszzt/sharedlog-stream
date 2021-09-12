//go:generate greenpack
//msgp:ignore PersonTimeJSONEncoder PersonTimeJSONDecoder PersonTimeJSONSerde
//msgp:ignore PersonTimeMsgpEncoder PersonTimeMsgpDecoder PersonTimeMsgpSerde
package types

import (
	"encoding/json"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
)

type PersonTime struct {
	ID        uint64 `json:"id" zid:"0" msg:"id"`
	Name      string `json:"name" zid:"1" msg:"name"`
	StartTime uint64 `json:"startTime" zid:"2" msg:"startTime"`
}

type PersonTimeJSONEncoder struct{}

var _ = processor.Encoder(PersonTimeJSONEncoder{})

func (e PersonTimeJSONEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*PersonTime)
	return json.Marshal(se)
}

type PersonTimeJSONDecoder struct{}

var _ = processor.Decoder(PersonTimeJSONDecoder{})

func (d PersonTimeJSONDecoder) Decode(value []byte) (interface{}, error) {
	se := &PersonTime{}
	err := json.Unmarshal(value, se)
	if err != nil {
		return nil, err
	}
	return se, nil
}

type PersonTimeJSONSerde struct {
	PersonTimeJSONEncoder
	PersonTimeJSONDecoder
}

type PersonTimeMsgpEncoder struct{}

var _ = processor.Encoder(PersonTimeMsgpEncoder{})

func (e PersonTimeMsgpEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*PersonTime)
	return se.MarshalMsg(nil)
}

type PersonTimeMsgpDecoder struct{}

var _ = processor.Decoder(PersonTimeMsgpDecoder{})

func (d PersonTimeMsgpDecoder) Decode(value []byte) (interface{}, error) {
	se := &PersonTime{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return se, nil
}

type PersonTimeMsgpSerde struct {
	PersonTimeMsgpEncoder
	PersonTimeMsgpDecoder
}
