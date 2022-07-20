//go:generate msgp
//msgp:ignore MessageMsgpSerde MessageJSONSerde

package commtypes

import (
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/pkg/utils"
)

type MessageSerde[K, V any] interface {
	Serde[Message]
	GetKeySerde() Serde[K]
	GetValSerde() Serde[V]
}

type MessageSerialized struct {
	KeyEnc    []byte `json:"key,omitempty" msg:"key,omitempty"`
	ValueEnc  []byte `json:"val,omitempty" msg:"val,omitempty"`
	InjT      int64  `msg:"injT" json:"injT"`
	Timestamp int64  `msg:"ts,omitempty" json:"ts,omitempty"`
}

func convertToMsgSer[K, V any](value Message, keySerde Serde[K], valSerde Serde[V]) (*MessageSerialized, error) {
	v := &value
	var err error

	var kenc []byte
	if !utils.IsNil(v.Key) {
		kenc, err = keySerde.Encode(v.Key.(K))
		if err != nil {
			return nil, fmt.Errorf("fail to encode key: %v", err)
		}
	}

	var venc []byte
	if !utils.IsNil(v.Value) {
		venc, err = valSerde.Encode(v.Value.(V))
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

func decodeToMsg[K, V any](msgSer *MessageSerialized, keySerde Serde[K], valSerde Serde[V]) (Message, error) {
	var err error
	var key K
	if msgSer.KeyEnc != nil {
		key, err = keySerde.Decode(msgSer.KeyEnc)
		if err != nil {
			return Message{}, fmt.Errorf("fail to decode key: %v", err)
		}
	}
	var val V
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

type MessageMsgpSerde[K, V any] struct {
	KeySerde Serde[K]
	ValSerde Serde[V]
}

var _ = MessageSerde[int, int](MessageMsgpSerde[int, int]{})

func (s MessageMsgpSerde[K, V]) GetKeySerde() Serde[K] {
	return s.KeySerde
}

func (s MessageMsgpSerde[K, V]) GetValSerde() Serde[V] {
	return s.ValSerde
}

func (s MessageMsgpSerde[K, V]) Encode(value Message) ([]byte, error) {
	msg, err := convertToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return msg.MarshalMsg(nil)
}

func (s MessageMsgpSerde[K, V]) Decode(value []byte) (Message, error) {
	msgSer := MessageSerialized{}
	_, err := msgSer.UnmarshalMsg(value)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[ERROR] fail to unmarshal this msg: %v", string(value))
		return Message{}, fmt.Errorf("fail to unmarshal msg: %v", err)
	}
	return decodeToMsg(&msgSer, s.KeySerde, s.ValSerde)
}

type MessageJSONSerde[K, V any] struct {
	KeySerde Serde[K]
	ValSerde Serde[V]
}

var _ = MessageSerde[int, int](MessageJSONSerde[int, int]{})

func (s MessageJSONSerde[K, V]) GetKeySerde() Serde[K] {
	return s.KeySerde
}

func (s MessageJSONSerde[K, V]) GetValSerde() Serde[V] {
	return s.ValSerde
}

func (s MessageJSONSerde[K, V]) Encode(value Message) ([]byte, error) {
	msg, err := convertToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return json.Marshal(msg)
}

func (s MessageJSONSerde[K, V]) Decode(value []byte) (Message, error) {
	msgSer := MessageSerialized{}
	if err := json.Unmarshal(value, &msgSer); err != nil {
		return Message{}, err
	}
	return decodeToMsg(&msgSer, s.KeySerde, s.ValSerde)
}

func GetMsgSerde[K, V any](serdeFormat SerdeFormat, keySerde Serde[K], valSerde Serde[V]) (MessageSerde[K, V], error) {
	if serdeFormat == JSON {
		return MessageJSONSerde[K, V]{
			KeySerde: keySerde,
			ValSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return MessageMsgpSerde[K, V]{
			KeySerde: keySerde,
			ValSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
