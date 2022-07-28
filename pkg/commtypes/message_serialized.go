//go:generate msgp
//msgp:ignore MessageMsgpSerde MessageJSONSerde

package commtypes

import (
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/pkg/utils"
)

type MessageSerde interface {
	Serde
	SerdeG[interface{}]
	EncodeAndRtnKVBin(value interface{}) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error)
	EncodeKey(key interface{}) ([]byte, error)
	EncodeVal(val interface{}) ([]byte, error)
	DecodeVal(value []byte) (interface{}, error)
}

type MessageSerdeG[K, V any] interface {
	SerdeG[Message]
	EncodeAndRtnKVBin(value Message) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error)
	EncodeKey(key K) ([]byte, error)
	EncodeVal(key V) ([]byte, error)
	DecodeVal(value []byte) (V, error)
	GetKeySerdeG() SerdeG[K]
	GetValSerdeG() SerdeG[V]
}

type MessageSerialized struct {
	KeyEnc    []byte `json:"key,omitempty" msg:"key,omitempty"`
	ValueEnc  []byte `json:"val,omitempty" msg:"val,omitempty"`
	InjT      int64  `msg:"injT" json:"injT"`
	Timestamp int64  `msg:"ts,omitempty" json:"ts,omitempty"`
}

func convertToMsgSer(value interface{}, keySerde Serde, valSerde Serde) (*MessageSerialized, error) {
	if value == nil {
		return nil, nil
	}
	v, ok := value.(*Message)
	if !ok {
		vtmp := value.(Message)
		v = &vtmp
	}
	if v == nil {
		return nil, nil
	}
	var err error

	var kenc []byte
	if !utils.IsNil(v.Key) {
		kenc, err = keySerde.Encode(v.Key)
		if err != nil {
			return nil, fmt.Errorf("fail to encode key: %v", err)
		}
	}

	var venc []byte
	if !utils.IsNil(v.Value) {
		venc, err = valSerde.Encode(v.Value)
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
		InjT:      v.InjT,
		Timestamp: v.Timestamp,
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

func decodeToMsg(msgSer *MessageSerialized, keySerde Serde, valSerde Serde) (interface{}, error) {
	var err error
	var key interface{} = nil
	if msgSer.KeyEnc != nil {
		key, err = keySerde.Decode(msgSer.KeyEnc)
		if err != nil {
			return nil, fmt.Errorf("fail to decode key: %v", err)
		}
	}
	var val interface{} = nil
	if msgSer.ValueEnc != nil {
		val, err = valSerde.Decode(msgSer.ValueEnc)
		if err != nil {
			return nil, fmt.Errorf("fail to decode val: %v", err)
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

type MessageMsgpSerde struct {
	keySerde Serde
	valSerde Serde
}

var _ MessageSerde = &MessageMsgpSerde{}

func (s MessageMsgpSerde) Encode(value interface{}) ([]byte, error) {
	msgSer, err := convertToMsgSer(value, s.keySerde, s.valSerde)
	if err != nil {
		return nil, err
	}
	if msgSer == nil {
		return nil, nil
	}
	return msgSer.MarshalMsg(nil)
}

func (s MessageMsgpSerde) EncodeAndRtnKVBin(value interface{}) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error) {
	msgSer, err := convertToMsgSer(value, s.keySerde, s.valSerde)
	if err != nil {
		return nil, nil, nil, err
	}
	if msgSer == nil {
		return nil, nil, nil, nil
	}
	msgEnc, err := msgSer.MarshalMsg(nil)
	return msgEnc, msgSer.KeyEnc, msgSer.ValueEnc, err
}

func (s MessageMsgpSerde) EncodeKey(key interface{}) ([]byte, error) {
	if key == nil {
		return nil, nil
	}
	return s.keySerde.Encode(key)
}

func (s MessageMsgpSerde) DecodeVal(val []byte) (interface{}, error) {
	return s.valSerde.Decode(val)
}

func (s MessageMsgpSerde) EncodeVal(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	return s.valSerde.Encode(value)
}

func (s MessageMsgpSerde) Decode(value []byte) (interface{}, error) {
	msgSer := MessageSerialized{}
	_, err := msgSer.UnmarshalMsg(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] fail to unmarshal this msg: %v", string(value))
		return nil, fmt.Errorf("fail to unmarshal msg: %v", err)
	}
	return decodeToMsg(&msgSer, s.keySerde, s.valSerde)
}

type MessageMsgpSerdeG[K, V any] struct {
	keySerde SerdeG[K]
	valSerde SerdeG[V]
}

var _ MessageSerdeG[int, int] = MessageMsgpSerdeG[int, int]{}

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

type MessageJSONSerde struct {
	KeySerde Serde
	ValSerde Serde
}

var _ MessageSerde = MessageJSONSerde{}

func (s MessageJSONSerde) Encode(value interface{}) ([]byte, error) {
	msg, err := convertToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return json.Marshal(msg)
}
func (s MessageJSONSerde) EncodeAndRtnKVBin(value interface{}) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error) {
	msg, err := convertToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, nil, nil, err
	}
	if msg == nil {
		return nil, nil, nil, nil
	}
	msgEnc, err := json.Marshal(msg)
	return msgEnc, msg.KeyEnc, msg.ValueEnc, err
}

func (s MessageJSONSerde) EncodeKey(key interface{}) ([]byte, error) {
	return s.KeySerde.Encode(key)
}

func (s MessageJSONSerde) EncodeVal(value interface{}) ([]byte, error) {
	return s.ValSerde.Encode(value)
}

func (s MessageJSONSerde) DecodeVal(v []byte) (interface{}, error) {
	return s.ValSerde.Decode(v)
}

func (s MessageJSONSerde) Decode(value []byte) (interface{}, error) {
	msgSer := MessageSerialized{}
	if err := json.Unmarshal(value, &msgSer); err != nil {
		return nil, err
	}
	return decodeToMsg(&msgSer, s.KeySerde, s.ValSerde)
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

func GetMsgSerde(serdeFormat SerdeFormat, keySerde Serde, valSerde Serde) (MessageSerde, error) {
	if serdeFormat == JSON {
		return MessageJSONSerde{
			KeySerde: keySerde,
			ValSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return MessageMsgpSerde{
			keySerde: keySerde,
			valSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
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
