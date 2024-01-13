//go:generate msgp
//msgp:ignore CheckpointJSONSerdeG CheckpointMsgpSerdeG
package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type ChkptMetaData struct {
	Unprocessed []uint64 `json:"up,omitempty" msg:"up,omitempty"`
	// stores the last marker seqnumber for align checkpoint
	LastChkptMarker uint64 `json:"lcpm,omitempty" msg:"lcpm,omitempty"`
}

type Checkpoint struct {
	KvArr     [][]byte        `json:"kvarr,omitempty" msg:"kvarr,omitempty"`
	ChkptMeta []ChkptMetaData `json:"chkptm,omitempty" msg:"chkptm,omitempty"`
}

type (
	CheckpointJSONSerdeG    struct{}
	CheckpointPtrJSONSerdeG struct{}
	CheckpointMsgpSerdeG    struct{}
)

var (
	_ = SerdeG[Checkpoint](CheckpointJSONSerdeG{})
	_ = SerdeG[Checkpoint](CheckpointMsgpSerdeG{})
	_ = SerdeG[*Checkpoint](CheckpointPtrJSONSerdeG{})
)

func (s CheckpointJSONSerdeG) Encode(v Checkpoint) ([]byte, error) {
	return json.Marshal(&v)
}

func (s CheckpointJSONSerdeG) Decode(value []byte) (Checkpoint, error) {
	v := Checkpoint{}
	if err := json.Unmarshal(value, &v); err != nil {
		return Checkpoint{}, err
	}
	return v, nil
}

func (s CheckpointPtrJSONSerdeG) Encode(v *Checkpoint) ([]byte, error) {
	return json.Marshal(&v)
}

func (s CheckpointPtrJSONSerdeG) Decode(value []byte) (*Checkpoint, error) {
	v := Checkpoint{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

func (s CheckpointMsgpSerdeG) Encode(v Checkpoint) ([]byte, error) {
	return v.MarshalMsg(nil)
}

func (s CheckpointMsgpSerdeG) Decode(value []byte) (Checkpoint, error) {
	val := Checkpoint{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return Checkpoint{}, err
	}
	return val, nil
}

func GetCheckpointSerdeG(serdeFormat SerdeFormat) (SerdeG[Checkpoint], error) {
	switch serdeFormat {
	case JSON:
		return CheckpointJSONSerdeG{}, nil
	case MSGP:
		return CheckpointMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
