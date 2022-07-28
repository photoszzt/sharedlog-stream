package commtypes

import (
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/pkg/utils"
)

type MessageSerdeG[K, V any] interface {
	SerdeG[Message]
	EncodeAndRtnKVBin(value Message) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error)
	EncodeKey(key K) ([]byte, error)
	EncodeVal(key V) ([]byte, error)
	DecodeVal(value []byte) (V, error)
	GetKeySerdeG() SerdeG[K]
	GetValSerdeG() SerdeG[V]
}

type MessageMsgpSerdeG[K, V any] struct {
	keySerde SerdeG[K]
	valSerde SerdeG[V]
}

var _ MessageSerdeG[int, int] = MessageMsgpSerdeG[int, int]{}

func msgSerToMsg[K, V any](msgSer *MessageSerialized, keySerde SerdeG[K], valSerde SerdeG[V]) (Message, error) {
	var err error
	var key interface{} = nil
	if msgSer.KeyEnc != nil {
		key, err = keySerde.Decode(msgSer.KeyEnc)
		if err != nil {
			return Message{}, fmt.Errorf("fail to decode key: %v", err)
		}
	}
	var val interface{} = nil
	if msgSer.ValueEnc != nil {
		val, err = valSerde.Decode(msgSer.ValueEnc)
		if err != nil {
			return Message{}, fmt.Errorf("fail to decode val: %v", err)
		}
	}
	msg := Message{
		Key:       key,
		Value:     val,
		InjT:      msgSer.InjT,
		Timestamp: msgSer.Timestamp,
	}
	return msg, nil
}

func msgToMsgSer[K, V any](value Message, keySerde SerdeG[K], valSerde SerdeG[V]) (*MessageSerialized, error) {
	var err error

	var kenc []byte
	if !utils.IsNil(value.Key) {
		kenc, err = keySerde.Encode(value.Key.(K))
		if err != nil {
			return nil, fmt.Errorf("fail to encode key: %v", err)
		}
	}

	var venc []byte
	if !utils.IsNil(value.Value) {
		venc, err = valSerde.Encode(value.Value.(V))
		if err != nil {
			return nil, fmt.Errorf("fail encode val: %v", err)
		}
	}
	if kenc == nil && venc == nil {
		return nil, nil
	}
	msg := &MessageSerialized{
		KeyEnc:    kenc,
		ValueEnc:  venc,
		InjT:      value.InjT,
		Timestamp: value.Timestamp,
	}
	return msg, nil
}

func (s MessageMsgpSerdeG[K, V]) Encode(value Message) ([]byte, error) {
	msg, err := msgToMsgSer(value, s.keySerde, s.valSerde)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return msg.MarshalMsg(nil)
}

func (s MessageMsgpSerdeG[K, V]) EncodeKey(key K) ([]byte, error) {
	return s.keySerde.Encode(key)
}

func (s MessageMsgpSerdeG[K, V]) EncodeVal(value V) ([]byte, error) {
	return s.valSerde.Encode(value)
}

func (s MessageMsgpSerdeG[K, V]) DecodeVal(v []byte) (V, error) {
	return s.valSerde.Decode(v)
}

func (s MessageMsgpSerdeG[K, V]) GetKeySerdeG() SerdeG[K] {
	return s.keySerde
}

func (s MessageMsgpSerdeG[K, V]) GetValSerdeG() SerdeG[V] {
	return s.valSerde
}

func (s MessageMsgpSerdeG[K, V]) EncodeAndRtnKVBin(value Message) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error) {
	msgSer, err := msgToMsgSer(value, s.keySerde, s.valSerde)
	if err != nil {
		return nil, nil, nil, err
	}
	if msgSer == nil {
		return nil, nil, nil, err
	}
	msgEnc, err := msgSer.MarshalMsg(nil)
	return msgEnc, msgSer.KeyEnc, msgSer.ValueEnc, err
}

func (s MessageMsgpSerdeG[K, V]) Decode(value []byte) (Message, error) {
	msgSer := MessageSerialized{}
	_, err := msgSer.UnmarshalMsg(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] fail to unmarshal this msg: %v", string(value))
		return Message{}, fmt.Errorf("fail to unmarshal msg: %v", err)
	}
	return msgSerToMsg(&msgSer, s.keySerde, s.valSerde)
}

type MessageJSONSerdeG[K, V any] struct {
	KeySerde SerdeG[K]
	ValSerde SerdeG[V]
}

var _ MessageSerdeG[int, int] = MessageJSONSerdeG[int, int]{}

func (s MessageJSONSerdeG[K, V]) Encode(value Message) ([]byte, error) {
	msg, err := msgToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return json.Marshal(msg)
}
func (s MessageJSONSerdeG[K, V]) EncodeAndRtnKVBin(value Message) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error) {
	msgSer, err := msgToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, nil, nil, err
	}
	if msgSer == nil {
		return nil, nil, nil, nil
	}
	msgEnc, err := json.Marshal(msgSer)
	return msgEnc, msgSer.KeyEnc, msgSer.ValueEnc, err
}

func (s MessageJSONSerdeG[K, V]) EncodeKey(key K) ([]byte, error) {
	return s.KeySerde.Encode(key)
}

func (s MessageJSONSerdeG[K, V]) EncodeVal(value V) ([]byte, error) {
	return s.ValSerde.Encode(value)
}

func (s MessageJSONSerdeG[K, V]) DecodeVal(v []byte) (V, error) {
	return s.ValSerde.Decode(v)
}

func (s MessageJSONSerdeG[K, V]) GetKeySerdeG() SerdeG[K] {
	return s.KeySerde
}

func (s MessageJSONSerdeG[K, V]) GetValSerdeG() SerdeG[V] {
	return s.ValSerde
}

func (s MessageJSONSerdeG[K, V]) Decode(value []byte) (Message, error) {
	msgSer := MessageSerialized{}
	if err := json.Unmarshal(value, &msgSer); err != nil {
		return Message{}, err
	}
	return msgSerToMsg(&msgSer, s.KeySerde, s.ValSerde)
}

func GetMsgSerdeG[K, V any](serdeFormat SerdeFormat, keySerde SerdeG[K], valSerde SerdeG[V]) (MessageSerdeG[K, V], error) {
	if serdeFormat == JSON {
		return MessageJSONSerdeG[K, V]{
			KeySerde: keySerde,
			ValSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return MessageMsgpSerdeG[K, V]{
			keySerde: keySerde,
			valSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
