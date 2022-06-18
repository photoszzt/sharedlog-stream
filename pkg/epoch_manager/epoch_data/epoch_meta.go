//go:generate msgp
//msgp:ignore
package epoch_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type EpochMetaMark uint8

const (
	EPOCH_END = EpochMetaMark(iota)
	FENCE
)

type ProduceRange struct {
	Start uint64 `json:"s" msg:"s"`
	End   uint64 `json:"e" msg:"e"`
}

type EpochMeta struct {
	ConSeqNums   map[string]uint64                 `json:"ConSeqNum,omitempty" msg:"ConSeqNum,omitempty"`
	OutputRanges map[string]map[uint8]ProduceRange `json:"outRng,omitempty" msg:"outRng,omitempty"`
	TaskId       uint64                            `json:"tid,omitempty" msg:"tid,omitempty"`
	TaskEpoch    uint16                            `json:"te,omitempty" msg:"te,omitempty"`
	Mark         EpochMetaMark                     `json:"mark,omitempty" msg:"mark,omitempty"`
}

type EpochMetaJSONSerde struct{}

func (s EpochMetaJSONSerde) Encode(value interface{}) ([]byte, error) {
	em := value.(*EpochMeta)
	return json.Marshal(em)
}
func (s EpochMetaJSONSerde) Decode(value []byte) (interface{}, error) {
	em := EpochMeta{}
	if err := json.Unmarshal(value, &em); err != nil {
		return nil, err
	}
	return em, nil
}

type EpochMetaMsgpSerde struct{}

func (s EpochMetaMsgpSerde) Encode(value interface{}) ([]byte, error) {
	em := value.(*EpochMeta)
	return em.MarshalMsg(nil)
}
func (s EpochMetaMsgpSerde) Decode(value []byte) (interface{}, error) {
	em := EpochMeta{}
	if _, err := em.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return em, nil
}

func GetEpochMetaSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serdeFormat == commtypes.JSON {
		return EpochMetaJSONSerde{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return EpochMetaMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
