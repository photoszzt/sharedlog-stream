package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PersonTimeJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[PersonTime](PersonTimeJSONSerdeG{})

func (s PersonTimeJSONSerdeG) Encode(value PersonTime) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s PersonTimeJSONSerdeG) Decode(value []byte) (PersonTime, error) {
	v := PersonTime{}
	if err := json.Unmarshal(value, &v); err != nil {
		return PersonTime{}, err
	}
	return v, nil
}

type PersonTimeMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[PersonTime](PersonTimeMsgpSerdeG{})

func (s PersonTimeMsgpSerdeG) Encode(value PersonTime) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s PersonTimeMsgpSerdeG) Decode(value []byte) (PersonTime, error) {
	v := PersonTime{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return PersonTime{}, err
	}
	return v, nil
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
