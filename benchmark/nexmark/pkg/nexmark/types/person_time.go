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

type PersonTimeJSONSerde struct{}
type PersonTimeJSONSerdeG struct{}

var _ = commtypes.Serde(PersonTimeJSONSerde{})
var _ = commtypes.SerdeG[PersonTime](PersonTimeJSONSerdeG{})

func (e PersonTimeJSONSerde) Encode(value interface{}) ([]byte, error) {
	se, ok := value.(*PersonTime)
	if !ok {
		seTmp := value.(PersonTime)
		se = &seTmp
	}
	return json.Marshal(se)
}

func (d PersonTimeJSONSerde) Decode(value []byte) (interface{}, error) {
	se := PersonTime{}
	err := json.Unmarshal(value, &se)
	if err != nil {
		return nil, err
	}
	return se, nil
}

func (e PersonTimeJSONSerdeG) Encode(value PersonTime) ([]byte, error) {
	return json.Marshal(&value)
}

func (d PersonTimeJSONSerdeG) Decode(value []byte) (PersonTime, error) {
	se := PersonTime{}
	err := json.Unmarshal(value, &se)
	if err != nil {
		return PersonTime{}, err
	}
	return se, nil
}

type PersonTimeMsgpSerde struct{}
type PersonTimeMsgpSerdeG struct{}

var _ = commtypes.Serde(PersonTimeMsgpSerde{})
var _ = commtypes.SerdeG[PersonTime](PersonTimeMsgpSerdeG{})

func (e PersonTimeMsgpSerde) Encode(value interface{}) ([]byte, error) {
	se, ok := value.(*PersonTime)
	if !ok {
		seTmp := value.(PersonTime)
		se = &seTmp
	}
	return se.MarshalMsg(nil)
}

func (d PersonTimeMsgpSerde) Decode(value []byte) (interface{}, error) {
	se := PersonTime{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return se, nil
}

func (e PersonTimeMsgpSerdeG) Encode(value PersonTime) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (d PersonTimeMsgpSerdeG) Decode(value []byte) (PersonTime, error) {
	se := PersonTime{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return PersonTime{}, err
	}
	return se, nil
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

func GetPersonTimeSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[PersonTime], error) {
	if serdeFormat == commtypes.JSON {
		return PersonTimeJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return PersonTimeMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
