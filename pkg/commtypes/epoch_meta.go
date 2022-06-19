//go:generate msgp
//msgp:ignore EpochMetaJSONSerde EpochMetaMsgpSerde
package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type EpochMark uint8

const (
	EPOCH_END = EpochMark(iota) // epoch end, for 2pc protocol, it's commit
	ABORT
	SCALE_FENCE
	FENCE
)

type ProduceRange struct {
	Start uint64 `json:"s" msg:"s"`
	End   uint64 `json:"e" msg:"e"`
}

type EpochMarker struct {
	ConSeqNums   map[string]uint64                 `json:"ConSeqNum,omitempty" msg:"ConSeqNum,omitempty"`
	OutputRanges map[string]map[uint8]ProduceRange `json:"outRng,omitempty" msg:"outRng,omitempty"`
	ScaleEpoch   uint64                            `json:"sepoch,omitempty" msg:"sepoch,omitempty"`
	Mark         EpochMark                         `json:"mark,omitempty" msg:"mark,omitempty"`
}

type EpochMarkerJSONSerde struct{}

func (s EpochMarkerJSONSerde) Encode(value interface{}) ([]byte, error) {
	em := value.(*EpochMarker)
	return json.Marshal(em)
}
func (s EpochMarkerJSONSerde) Decode(value []byte) (interface{}, error) {
	em := EpochMarker{}
	if err := json.Unmarshal(value, &em); err != nil {
		return nil, err
	}
	return em, nil
}

type EpochMarkerMsgpSerde struct{}

func (s EpochMarkerMsgpSerde) Encode(value interface{}) ([]byte, error) {
	em := value.(*EpochMarker)
	return em.MarshalMsg(nil)
}
func (s EpochMarkerMsgpSerde) Decode(value []byte) (interface{}, error) {
	em := EpochMarker{}
	if _, err := em.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return em, nil
}

func GetEpochMarkerSerde(serdeFormat SerdeFormat) (Serde, error) {
	if serdeFormat == JSON {
		return EpochMarkerJSONSerde{}, nil
	} else if serdeFormat == MSGP {
		return EpochMarkerMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
