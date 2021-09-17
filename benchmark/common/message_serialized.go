//go:generate greenpack
//msgpack:ignore MessageSerializedMsgpEncoder MessageSerializedMsgpDecoder
//msgpack:ignore MessageSerializedJSONEncoder MessageSerializedJSONDecoder

package common

import (
	"encoding/json"

	"sharedlog-stream/pkg/stream/processor"
)

type MessageSerialized struct {
	Key   []byte `json:",omitempty" zid:"0" msg:",omitempty"`
	Value []byte `json:",omitempty" zid:"1" msg:",omitempty"`
}

var _ = processor.MsgEncoder(MessageSerializedMsgpEncoder{})

type MessageSerializedMsgpEncoder struct{}

func (e MessageSerializedMsgpEncoder) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return msg.MarshalMsg(nil)
}

type MessageSerializedJSONEncoder struct{}

var _ = processor.MsgEncoder(MessageSerializedJSONEncoder{})

func (e MessageSerializedJSONEncoder) Encode(key []byte, value []byte) ([]byte, error) {
	msg := MessageSerialized{
		Key:   key,
		Value: value,
	}
	return json.Marshal(msg)
}

type MessageSerializedMsgpDecoder struct{}

var _ = processor.MsgDecoder(MessageSerializedMsgpDecoder{})

func (msmd MessageSerializedMsgpDecoder) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	_, err := msg.UnmarshalMsg(value)
	if err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}

type MessageSerializedJSONDecoder struct{}

var _ = processor.MsgDecoder(MessageSerializedJSONDecoder{})

func (msmd MessageSerializedJSONDecoder) Decode(value []byte) ([]byte /* key */, []byte /* value */, error) {
	msg := MessageSerialized{}
	if err := json.Unmarshal(value, &msg); err != nil {
		return nil, nil, err
	}
	return msg.Key, msg.Value, nil
}
