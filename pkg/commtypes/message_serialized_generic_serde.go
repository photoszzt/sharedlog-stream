package commtypes

import (
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/pkg/optional"
)

type MessageSerializedJSONSerdeG struct{}

var _ SerdeG[MessageSerialized] = MessageSerializedJSONSerdeG{}

func (s MessageSerializedJSONSerdeG) Encode(value MessageSerialized) ([]byte, error) {
	return json.Marshal(value)
}
func (s MessageSerializedJSONSerdeG) Decode(data []byte) (MessageSerialized, error) {
	var msg MessageSerialized
	err := json.Unmarshal(data, &msg)
	return msg, err
}

type MessageSerializedMsgpSerdeG struct{}

var _ SerdeG[MessageSerialized] = MessageSerializedMsgpSerdeG{}

func (s MessageSerializedMsgpSerdeG) Encode(value MessageSerialized) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s MessageSerializedMsgpSerdeG) Decode(data []byte) (MessageSerialized, error) {
	var msg MessageSerialized
	_, err := msg.UnmarshalMsg(data)
	return msg, err
}

func GetMessageSerializedSerdeG(serdeFormat SerdeFormat) SerdeG[MessageSerialized] {
	if serdeFormat == JSON {
		return MessageSerializedJSONSerdeG{}
	} else if serdeFormat == MSGP {
		return MessageSerializedMsgpSerdeG{}
	} else {
		return nil
	}
}

type MessageSerdeG[K, V any] interface {
	SerdeG[Message]
	EncodeWithKVBytes(kBytes []byte, vBytes []byte, inj int64, ts int64) ([]byte, error)
	EncodeKey(key K) ([]byte, error)
	EncodeVal(key V) ([]byte, error)
	DecodeVal(value []byte) (V, error)
	GetKeySerdeG() SerdeG[K]
	GetValSerdeG() SerdeG[V]
}

type MessageGSerdeG[K, V any] interface {
	SerdeG[MessageG[K, V]]
	EncodeWithKVBytes(kBytes []byte, vBytes []byte, inj int64, ts int64) ([]byte, error)
	EncodeKey(key K) ([]byte, error)
	EncodeVal(key V) ([]byte, error)
	DecodeVal(value []byte) (V, error)
	GetKeySerdeG() SerdeG[K]
	GetValSerdeG() SerdeG[V]
}

func MsgSerToMsgG[K, V any](msgSer *MessageSerialized, keySerde SerdeG[K], valSerde SerdeG[V]) (MessageG[K, V], error) {
	key := optional.None[K]()
	if msgSer.KeyEnc != nil {
		k, err := keySerde.Decode(msgSer.KeyEnc)
		if err != nil {
			return MessageG[K, V]{}, fmt.Errorf("fail to decode key: %v", err)
		}
		key = optional.Some(k)
	}
	val := optional.None[V]()
	if msgSer.ValueEnc != nil {
		v, err := valSerde.Decode(msgSer.ValueEnc)
		if err != nil {
			return MessageG[K, V]{}, fmt.Errorf("fail to decode val: %v", err)
		}
		val = optional.Some(v)
	}
	msg := MessageG[K, V]{
		Key:       key,
		Value:     val,
		InjT:      msgSer.InjT,
		Timestamp: msgSer.Timestamp,
	}
	return msg, nil
}

func MsgGToMsgSer[K, V any](value MessageG[K, V], keySerde SerdeG[K], valSerde SerdeG[V]) (optional.Option[MessageSerialized], error) {
	var err error

	var kenc []byte
	k, ok := value.Key.Take()
	if ok {
		kenc, err = keySerde.Encode(k)
		if err != nil {
			return optional.None[MessageSerialized](), fmt.Errorf("fail to encode key: %v", err)
		}
	}

	var venc []byte
	v, ok := value.Value.Take()
	if ok {
		venc, err = valSerde.Encode(v)
		if err != nil {
			return optional.None[MessageSerialized](), fmt.Errorf("fail encode val: %v", err)
		}
	}
	if kenc == nil && venc == nil {
		return optional.None[MessageSerialized](), nil
	}
	msg := optional.Some(MessageSerialized{
		KeyEnc:    kenc,
		ValueEnc:  venc,
		InjT:      value.InjT,
		Timestamp: value.Timestamp,
	})
	return msg, nil
}

type MessageGMsgpSerdeG[K, V any] struct {
	keySerde SerdeG[K]
	valSerde SerdeG[V]
}

var _ MessageGSerdeG[int, int] = MessageGMsgpSerdeG[int, int]{}

func (s MessageGMsgpSerdeG[K, V]) Encode(val MessageG[K, V]) ([]byte, error) {
	msgSerOp, err := MsgGToMsgSer(val, s.keySerde, s.valSerde)
	if err != nil {
		return nil, err
	}
	msgSer, ok := msgSerOp.Take()
	if !ok {
		return nil, nil
	}
	return msgSer.MarshalMsg(nil)
}
func (s MessageGMsgpSerdeG[K, V]) Decode(value []byte) (MessageG[K, V], error) {
	msgSer := MessageSerialized{}
	_, err := msgSer.UnmarshalMsg(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] fail to unmarshal this msg2: %v\n", string(value))
		return MessageG[K, V]{}, fmt.Errorf("fail to unmarshal msg: %v", err)
	}
	return MsgSerToMsgG(&msgSer, s.keySerde, s.valSerde)
}
func (s MessageGMsgpSerdeG[K, V]) EncodeWithKVBytes(kBytes []byte, vBytes []byte, inj int64, ts int64) ([]byte, error) {
	msgSer := &MessageSerialized{
		KeyEnc:    kBytes,
		ValueEnc:  vBytes,
		InjT:      inj,
		Timestamp: ts,
	}
	return msgSer.MarshalMsg(nil)
}
func (s MessageGMsgpSerdeG[K, V]) EncodeKey(key K) ([]byte, error)   { return s.keySerde.Encode(key) }
func (s MessageGMsgpSerdeG[K, V]) EncodeVal(key V) ([]byte, error)   { return s.valSerde.Encode(key) }
func (s MessageGMsgpSerdeG[K, V]) DecodeVal(value []byte) (V, error) { return s.valSerde.Decode(value) }
func (s MessageGMsgpSerdeG[K, V]) GetKeySerdeG() SerdeG[K]           { return s.keySerde }
func (s MessageGMsgpSerdeG[K, V]) GetValSerdeG() SerdeG[V]           { return s.valSerde }

type MessageGJSONSerdeG[K, V any] struct {
	KeySerde SerdeG[K]
	ValSerde SerdeG[V]
}

var _ MessageGSerdeG[int, int] = MessageGJSONSerdeG[int, int]{}

func (s MessageGJSONSerdeG[K, V]) Encode(value MessageG[K, V]) ([]byte, error) {
	msgSerOp, err := MsgGToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, err
	}
	msgSer, ok := msgSerOp.Take()
	if !ok {
		return nil, nil
	}
	return json.Marshal(msgSer)
}

func (s MessageGJSONSerdeG[K, V]) Decode(value []byte) (MessageG[K, V], error) {
	msgSer := MessageSerialized{}
	if err := json.Unmarshal(value, &msgSer); err != nil {
		return MessageG[K, V]{}, fmt.Errorf("[JSON] fail to unmarshal to msgSer: %v", err)
	}
	return MsgSerToMsgG(&msgSer, s.KeySerde, s.ValSerde)
}
func (s MessageGJSONSerdeG[K, V]) EncodeWithKVBytes(kBytes []byte, vBytes []byte, inj int64, ts int64) ([]byte, error) {
	msgSer := &MessageSerialized{
		KeyEnc:    kBytes,
		ValueEnc:  vBytes,
		InjT:      inj,
		Timestamp: ts,
	}
	return msgSer.MarshalMsg(nil)
}
func (s MessageGJSONSerdeG[K, V]) EncodeKey(key K) ([]byte, error)   { return s.KeySerde.Encode(key) }
func (s MessageGJSONSerdeG[K, V]) EncodeVal(value V) ([]byte, error) { return s.ValSerde.Encode(value) }
func (s MessageGJSONSerdeG[K, V]) DecodeVal(value []byte) (V, error) { return s.ValSerde.Decode(value) }
func (s MessageGJSONSerdeG[K, V]) GetKeySerdeG() SerdeG[K]           { return s.KeySerde }
func (s MessageGJSONSerdeG[K, V]) GetValSerdeG() SerdeG[V]           { return s.ValSerde }

func GetMsgGSerdeG[K, V any](serdeFormat SerdeFormat, keySerde SerdeG[K], valSerde SerdeG[V]) (MessageGSerdeG[K, V], error) {
	if serdeFormat == JSON {
		return MessageGJSONSerdeG[K, V]{
			KeySerde: keySerde,
			ValSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return MessageGMsgpSerdeG[K, V]{
			keySerde: keySerde,
			valSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
