//go:generate msgp
//msgp:ignore SumAndCountJSONSerde SumAndCountMsgpSerde
package ntypes

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

func SizeOfSumAndCount(k SumAndCount) int64 {
	return 16
}

func SizeOfSumAndCountPtr(k *SumAndCount) int64 {
	return 16
}

type SumAndCountJSONSerde struct {
	commtypes.DefaultJSONSerde
}

type SumAndCountMsgpSerde struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.Serde(SumAndCountJSONSerde{})

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

var _ = commtypes.Serde(SumAndCountMsgpSerde{})

func (s SumAndCountMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*SumAndCount)
	b := commtypes.PopBuffer()
	buf := *b
	return val.MarshalMsg(buf[:0])
}

func (s SumAndCountMsgpSerde) Decode(value []byte) (interface{}, error) {
	sc := SumAndCount{}
	if _, err := sc.UnmarshalMsg(value); err != nil {
		return nil, err
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
