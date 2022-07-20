//go:generate msgp
//msgp:ignore PersonTimeJSONEncoder PersonTimeJSONDecoder PersonTimeJSONSerde
//msgp:ignore PersonTimeMsgpEncoder PersonTimeMsgpDecoder PersonTimeMsgpSerde
package types

import (
	"encoding/json"
	"fmt"

	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PersonTime struct {
	Name      string `json:"name" msg:"name"`
	ID        uint64 `json:"id" msg:"id"`
	StartTime int64  `json:"startTime" msg:"startTime"`
}

var _ = fmt.Stringer(PersonTime{})

func (pt PersonTime) String() string {
	return fmt.Sprintf("PersonTime: {Name: %s, ID: %d, StartTime: %d}",
		pt.Name, pt.ID, pt.StartTime)
}

type PersonTimeJSONEncoder struct{}

var _ = commtypes.Encoder(PersonTimeJSONEncoder{})

func (e PersonTimeJSONEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*PersonTime)
	return json.Marshal(se)
}

type PersonTimeJSONDecoder struct{}

var _ = commtypes.Decoder(PersonTimeJSONDecoder{})

func (d PersonTimeJSONDecoder) Decode(value []byte) (interface{}, error) {
	se := PersonTime{}
	err := json.Unmarshal(value, &se)
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

var _ = commtypes.Encoder(PersonTimeMsgpEncoder{})

func (e PersonTimeMsgpEncoder) Encode(value interface{}) ([]byte, error) {
	se := value.(*PersonTime)
	return se.MarshalMsg(nil)
}

type PersonTimeMsgpDecoder struct{}

var _ = commtypes.Decoder(PersonTimeMsgpDecoder{})

func (d PersonTimeMsgpDecoder) Decode(value []byte) (interface{}, error) {
	se := PersonTime{}
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

func GetPersonTimeSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	var ptSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		ptSerde = PersonTimeJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		ptSerde = PersonTimeMsgpSerde{}
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
	return ptSerde, nil
}
