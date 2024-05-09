package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PriceTimeListJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[PriceTimeList](PriceTimeListJSONSerdeG{})

func (s PriceTimeListJSONSerdeG) Encode(value PriceTimeList) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s PriceTimeListJSONSerdeG) Decode(value []byte) (PriceTimeList, error) {
	v := PriceTimeList{}
	if err := json.Unmarshal(value, &v); err != nil {
		return PriceTimeList{}, err
	}
	return v, nil
}

type PriceTimeListMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[PriceTimeList](PriceTimeListMsgpSerdeG{})

func (s PriceTimeListMsgpSerdeG) Encode(value PriceTimeList) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s PriceTimeListMsgpSerdeG) Decode(value []byte) (PriceTimeList, error) {
	v := PriceTimeList{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return PriceTimeList{}, err
	}
	return v, nil
}

func GetPriceTimeListSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[PriceTimeList], error) {
	if serdeFormat == commtypes.JSON {
		return PriceTimeListJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return PriceTimeListMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
