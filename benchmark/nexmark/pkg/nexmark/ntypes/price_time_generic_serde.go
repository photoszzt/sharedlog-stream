package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PriceTimePtrJSONSerdeG struct{}

var _ = commtypes.SerdeG[*PriceTime](PriceTimePtrJSONSerdeG{})

func (s PriceTimePtrJSONSerdeG) Encode(value *PriceTime) ([]byte, error) {
	return json.Marshal(&value)
}

func (s PriceTimePtrJSONSerdeG) Decode(value []byte) (*PriceTime, error) {
	pt := PriceTime{}
	if err := json.Unmarshal(value, &pt); err != nil {
		return nil, err
	}
	return &pt, nil
}

type PriceTimePtrMsgpSerdeG struct{}

var _ = commtypes.SerdeG[*PriceTime](PriceTimePtrMsgpSerdeG{})

func (s PriceTimePtrMsgpSerdeG) Encode(value *PriceTime) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s PriceTimePtrMsgpSerdeG) Decode(value []byte) (*PriceTime, error) {
	pt := PriceTime{}
	if _, err := pt.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &pt, nil
}

func GetPriceTimeSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*PriceTime], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return PriceTimePtrJSONSerdeG{}, nil
	case commtypes.MSGP:
		return PriceTimePtrMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
