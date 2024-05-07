package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PriceTimeListJSONSerdeG struct{}

var _ = commtypes.SerdeG[PriceTimeList](PriceTimeListJSONSerdeG{})

func (s PriceTimeListJSONSerdeG) Encode(value PriceTimeList) ([]byte, error) {
	return json.Marshal(&value)
}

func (s PriceTimeListJSONSerdeG) Decode(value []byte) (PriceTimeList, error) {
	ptl := PriceTimeList{}
	err := json.Unmarshal(value, &ptl)
	if err != nil {
		return PriceTimeList{}, err
	}
	return ptl, nil
}

type PriceTimeListMsgpSerdeG struct{}

var _ = commtypes.SerdeG[PriceTimeList](PriceTimeListMsgpSerdeG{})

func (s PriceTimeListMsgpSerdeG) Encode(value PriceTimeList) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s PriceTimeListMsgpSerdeG) Decode(value []byte) (PriceTimeList, error) {
	ptl := PriceTimeList{}
	_, err := ptl.UnmarshalMsg(value)
	if err != nil {
		return PriceTimeList{}, err
	}
	return ptl, nil
}

func GetPriceTimeListSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[PriceTimeList], error) {
	switch serdeFormat {
	case commtypes.JSON:
		return PriceTimeListJSONSerdeG{}, nil
	case commtypes.MSGP:
		return PriceTimeListMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
