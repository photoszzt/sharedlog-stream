package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PriceTimeJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[PriceTime](PriceTimeJSONSerdeG{})

func (s PriceTimeJSONSerdeG) Encode(value PriceTime) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s PriceTimeJSONSerdeG) Decode(value []byte) (PriceTime, error) {
	v := PriceTime{}
	if err := json.Unmarshal(value, &v); err != nil {
		return PriceTime{}, err
	}
	return v, nil
}

type PriceTimeMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[PriceTime](PriceTimeMsgpSerdeG{})

func (s PriceTimeMsgpSerdeG) Encode(value PriceTime) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s PriceTimeMsgpSerdeG) Decode(value []byte) (PriceTime, error) {
	v := PriceTime{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return PriceTime{}, err
	}
	return v, nil
}

func GetPriceTimeSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[PriceTime], error) {
	if serdeFormat == commtypes.JSON {
		return PriceTimeJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return PriceTimeMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
