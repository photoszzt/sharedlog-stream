package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PriceTimeJSONSerdeG struct{}

var _ = commtypes.SerdeG[PriceTime](PriceTimeJSONSerdeG{})

func (s PriceTimeJSONSerdeG) Encode(value PriceTime) ([]byte, error) {
	return json.Marshal(&value)
}

func (s PriceTimeJSONSerdeG) Decode(value []byte) (PriceTime, error) {
	pt := PriceTime{}
	if err := json.Unmarshal(value, &pt); err != nil {
		return PriceTime{}, err
	}
	return pt, nil
}

type PriceTimeMsgpSerdeG struct{}

var _ = commtypes.SerdeG[PriceTime](PriceTimeMsgpSerdeG{})

func (s PriceTimeMsgpSerdeG) Encode(value PriceTime) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s PriceTimeMsgpSerdeG) Decode(value []byte) (PriceTime, error) {
	pt := PriceTime{}
	if _, err := pt.UnmarshalMsg(value); err != nil {
		return PriceTime{}, err
	}
	return pt, nil
}

func GetPriceTimeSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[PriceTime], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return PriceTimeJSONSerdeG{}, nil
	case commtypes.MSGP:
		return PriceTimeMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
