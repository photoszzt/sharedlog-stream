//go:generate msgp
//msgp:ignore EpochMetaJSONSerde EpochMetaMsgpSerde EpochMetaMsgpSerdeG EpochJSONMsgpSerdeG
//go:generate stringer -type=EpochMark
package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
)

type EpochMark uint8

const (
	EMPTY     = EpochMark(iota)
	EPOCH_END // epoch end, for 2pc protocol, it's commit
	ABORT
	SCALE_FENCE
	FENCE
)

type ProduceRange struct {
	Start        uint64 `json:"s" msg:"s"`
	End          uint64 `json:"e" msg:"e"`
	SubStreamNum uint8  `json:"sNum" msg:"sNum"`
}

var _ = fmt.Stringer(ProduceRange{})

func (p ProduceRange) String() string {
	return fmt.Sprintf("ProduceRange: {Start: %d, End: %d}", p.Start, p.End)
}

type EpochMarker struct {
	ConSeqNums   map[string]uint64         `json:"ConSeqNum,omitempty" msg:"ConSeqNum,omitempty"`
	OutputRanges map[string][]ProduceRange `json:"outRng,omitempty" msg:"outRng,omitempty"`
	ScaleEpoch   uint64                    `json:"sepoch,omitempty" msg:"sepoch,omitempty"`
	Mark         EpochMark                 `json:"mark,omitempty" msg:"mark,omitempty"`
}

type EpochMarkerJSONSerde struct{}

var _ = Serde(EpochMarkerJSONSerde{})

func (s EpochMarkerJSONSerde) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
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

var _ = Serde(EpochMarkerMsgpSerde{})

func (s EpochMarkerMsgpSerde) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	em := value.(*EpochMarker)
	return em.MarshalMsg(nil)
}

func (s EpochMarkerMsgpSerde) Decode(value []byte) (interface{}, error) {
	em := EpochMarker{}
	if _, err := em.UnmarshalMsg(value); err != nil {
		return EpochMarker{}, err
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
