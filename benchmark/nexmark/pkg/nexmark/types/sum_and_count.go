//go:generate msgp
//msgp:ignore SumAndCountJSONSerde SumAndCountMsgpSerde
package types

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type SumAndCount struct {
	Sum   uint64 `json:"sum" msg:"sum"`
	Count uint64 `json:"count" msg:"count"`
}

var _ = fmt.Stringer(SumAndCount{})

func (sc SumAndCount) String() string {
	return fmt.Sprintf("SumAndCount: {Sum: %d, Count: %d}", sc.Sum, sc.Count)
}

type SumAndCountJSONSerde struct{}
type SumAndCountJSONSerdeG struct{}

var _ = commtypes.Serde(SumAndCountJSONSerde{})
var _ = commtypes.SerdeG[SumAndCount](SumAndCountJSONSerdeG{})

func (s SumAndCountJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*SumAndCount)
	return json.Marshal(val)
}

func (s SumAndCountJSONSerde) Decode(value []byte) (interface{}, error) {
	sc := SumAndCount{}
	if err := json.Unmarshal(value, &sc); err != nil {
		return nil, err
	}
	return sc, nil
}

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

type SumAndCountMsgpSerde struct{}
type SumAndCountMsgpSerdeG struct{}

var _ = commtypes.Serde(SumAndCountMsgpSerde{})
var _ = commtypes.SerdeG[SumAndCount](SumAndCountMsgpSerdeG{})

func (s SumAndCountMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*SumAndCount)
	return val.MarshalMsg(nil)
}

func (s SumAndCountMsgpSerde) Decode(value []byte) (interface{}, error) {
	sc := SumAndCount{}
	if _, err := sc.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return sc, nil
}

func (s SumAndCountMsgpSerdeG) Encode(value SumAndCount) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s SumAndCountMsgpSerdeG) Decode(value []byte) (SumAndCount, error) {
	sc := SumAndCount{}
	if _, err := sc.UnmarshalMsg(value); err != nil {
		return SumAndCount{}, err
	}
	return sc, nil
}

func GetSumAndCountSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serdeFormat == commtypes.JSON {
		return SumAndCountJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return SumAndCountMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
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
