package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PersonTimeJSONSerdeG struct{}

var _ = commtypes.SerdeG[PersonTime](PersonTimeJSONSerdeG{})

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

type PersonTimeMsgpSerdeG struct{}

var _ = commtypes.SerdeG[PersonTime](PersonTimeMsgpSerdeG{})

func (e PersonTimeMsgpSerdeG) Encode(value PersonTime) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (d PersonTimeMsgpSerdeG) Decode(value []byte) (PersonTime, error) {
	se := PersonTime{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return PersonTime{}, err
	}
	return se, nil
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
