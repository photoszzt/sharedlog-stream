package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type SumAndCountJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[SumAndCount](SumAndCountJSONSerdeG{})

func (s SumAndCountJSONSerdeG) Encode(value SumAndCount) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s SumAndCountJSONSerdeG) Decode(value []byte) (SumAndCount, error) {
	v := SumAndCount{}
	if err := json.Unmarshal(value, &v); err != nil {
		return SumAndCount{}, err
	}
	return v, nil
}

type SumAndCountMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[SumAndCount](SumAndCountMsgpSerdeG{})

func (s SumAndCountMsgpSerdeG) Encode(value SumAndCount) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s SumAndCountMsgpSerdeG) Decode(value []byte) (SumAndCount, error) {
	v := SumAndCount{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return SumAndCount{}, err
	}
	return v, nil
}

func GetSumAndCountSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[SumAndCount], error) {
	if serdeFormat == commtypes.JSON {
		return SumAndCountJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return SumAndCountMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
