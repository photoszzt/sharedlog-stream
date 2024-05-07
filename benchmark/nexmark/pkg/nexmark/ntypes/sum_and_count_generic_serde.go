package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type SumAndCountJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

type SumAndCountMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[SumAndCount](SumAndCountJSONSerdeG{})

func (s SumAndCountJSONSerdeG) Encode(value SumAndCount) ([]byte, error) {
	return json.Marshal(&value)
}

func (s SumAndCountJSONSerdeG) Decode(value []byte) (SumAndCount, error) {
	sc := SumAndCount{}
	if err := json.Unmarshal(value, &sc); err != nil {
		return SumAndCount{}, err
	}
	return sc, nil
}

var _ = commtypes.SerdeG[SumAndCount](SumAndCountMsgpSerdeG{})

func (s SumAndCountMsgpSerdeG) Encode(value SumAndCount) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s SumAndCountMsgpSerdeG) Decode(value []byte) (SumAndCount, error) {
	sc := SumAndCount{}
	if _, err := sc.UnmarshalMsg(value); err != nil {
		return SumAndCount{}, err
	}
	return sc, nil
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
