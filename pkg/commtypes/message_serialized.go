//go:generate msgp
//msgpack:ignore MessageSerializedMsgpEncoder MessageSerializedMsgpDecoder
//msgpack:ignore MessageSerializedJSONEncoder MessageSerializedJSONDecoder

package commtypes

import (
	"encoding/json"
	"fmt"
)

type MessageSerialized struct {
	Key   []byte `json:"key,omitempty" msg:"key,omitempty"`
	Value []byte `json:"val,omitempty" msg:"val,omitempty"`
}

var _ = MsgEncoder(MessageSerializedMsgpSerde{})

type MessageSerializedMsgpSerde struct{}

func (e MessageSerializedMsgpSerde) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return msg.MarshalMsg(nil)
}

type MessageSerializedJSONSerde struct{}

var _ = MsgEncoder(MessageSerializedJSONSerde{})

func (e MessageSerializedJSONSerde) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return json.Marshal(msg)
}

var _ = MsgDecoder(MessageSerializedMsgpSerde{})

func (msmd MessageSerializedMsgpSerde) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	_, err := msg.UnmarshalMsg(value)
	if err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}

var _ = MsgDecoder(MessageSerializedJSONSerde{})

func (msmd MessageSerializedJSONSerde) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}

func GetMsgSerde(serdeFormat SerdeFormat) (MsgSerde, error) {
	if serdeFormat == JSON {
		return MessageSerializedJSONSerde{}, nil
	} else if serdeFormat == MSGP {
		return MessageSerializedMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
