package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type CheckpointJSONSerdeG struct {
	DefaultJSONSerde
}

var _ = SerdeG[*Checkpoint](CheckpointJSONSerdeG{})

func (s CheckpointJSONSerdeG) Encode(value *Checkpoint) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s CheckpointJSONSerdeG) Decode(value []byte) (*Checkpoint, error) {
	v := Checkpoint{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type CheckpointMsgpSerdeG struct {
	DefaultMsgpSerde
}

var _ = SerdeG[*Checkpoint](CheckpointMsgpSerdeG{})

func (s CheckpointMsgpSerdeG) Encode(value *Checkpoint) ([]byte, *[]byte, error) {
	b := PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s CheckpointMsgpSerdeG) Decode(value []byte) (*Checkpoint, error) {
	v := Checkpoint{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetCheckpointSerdeG(serdeFormat SerdeFormat) (SerdeG[*Checkpoint], error) {
	if serdeFormat == JSON {
		return CheckpointJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return CheckpointMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
