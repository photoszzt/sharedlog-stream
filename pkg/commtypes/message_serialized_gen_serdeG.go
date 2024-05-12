package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type MessageSerializedJSONSerdeG struct {
	DefaultJSONSerde
}

var _ = SerdeG[MessageSerialized](MessageSerializedJSONSerdeG{})

func (s MessageSerializedJSONSerdeG) Encode(value MessageSerialized) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s MessageSerializedJSONSerdeG) Decode(value []byte) (MessageSerialized, error) {
	v := MessageSerialized{}
	if err := json.Unmarshal(value, &v); err != nil {
		return MessageSerialized{}, err
	}
	return v, nil
}

type MessageSerializedMsgpSerdeG struct {
	DefaultMsgpSerde
}

var _ = SerdeG[MessageSerialized](MessageSerializedMsgpSerdeG{})

func (s MessageSerializedMsgpSerdeG) Encode(value MessageSerialized) ([]byte, *[]byte, error) {
	b := PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s MessageSerializedMsgpSerdeG) Decode(value []byte) (MessageSerialized, error) {
	v := MessageSerialized{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return MessageSerialized{}, err
	}
	return v, nil
}

func GetMessageSerializedSerdeG(serdeFormat SerdeFormat) (SerdeG[MessageSerialized], error) {
	if serdeFormat == JSON {
		return MessageSerializedJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return MessageSerializedMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
