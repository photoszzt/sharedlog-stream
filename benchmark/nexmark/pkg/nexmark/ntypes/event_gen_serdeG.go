package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type EventJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[*Event](EventJSONSerdeG{})

func (s EventJSONSerdeG) Encode(value *Event) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s EventJSONSerdeG) Decode(value []byte) (*Event, error) {
	v := Event{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type EventMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[*Event](EventMsgpSerdeG{})

func (s EventMsgpSerdeG) Encode(value *Event) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s EventMsgpSerdeG) Decode(value []byte) (*Event, error) {
	v := Event{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetEventSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*Event], error) {
	if serdeFormat == commtypes.JSON {
		return EventJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return EventMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
