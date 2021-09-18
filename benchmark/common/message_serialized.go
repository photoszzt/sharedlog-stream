//go:generate greenpack
//msgpack:ignore MessageSerializedMsgpEncoder MessageSerializedMsgpDecoder
//msgpack:ignore MessageSerializedJSONEncoder MessageSerializedJSONDecoder

package common

import (
	"encoding/json"
	"fmt"

	"sharedlog-stream/pkg/stream/processor"
)

type MessageSerialized struct {
	Key   []byte `json:",omitempty" zid:"0" msg:",omitempty"`
	Value []byte `json:",omitempty" zid:"1" msg:",omitempty"`
}

var _ = processor.MsgEncoder(MessageSerializedMsgpSerde{})

type MessageSerializedMsgpSerde struct{}

func (e MessageSerializedMsgpSerde) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return msg.MarshalMsg(nil)
}

type MessageSerializedJSONSerde struct{}

var _ = processor.MsgEncoder(MessageSerializedJSONSerde{})

func (e MessageSerializedJSONSerde) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return json.Marshal(msg)
}

var _ = processor.MsgDecoder(MessageSerializedMsgpSerde{})

func (msmd MessageSerializedMsgpSerde) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	_, err := msg.UnmarshalMsg(value)
	if err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}

var _ = processor.MsgDecoder(MessageSerializedJSONSerde{})

func (msmd MessageSerializedJSONSerde) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}

func GetMsgSerde(serdeFormat uint8) (processor.MsgSerde, error) {
	if serdeFormat == uint8(JSON) {
		return MessageSerializedJSONSerde{}, nil
	} else if serdeFormat == uint8(MSGP) {
		return MessageSerializedMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
