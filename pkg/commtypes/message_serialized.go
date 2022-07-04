//go:generate msgp
//msgp:ignore MessageMsgpSerde MessageJSONSerde

package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/utils"
)

type MessageSerde interface {
	Serde
	GetKeySerde() Serde
	GetValSerde() Serde
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
			return nil, err
		}
	}

	var venc []byte
	if !utils.IsNil(v.Value) {
		venc, err = valSerde.Encode(v.Value)
		if err != nil {
			return nil, err
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

func decodeToMsg(msgSer *MessageSerialized, keySerde Serde, valSerde Serde) (interface{}, error) {
	var err error
	var key interface{} = nil
	if msgSer.KeyEnc != nil {
		key, err = keySerde.Decode(msgSer.KeyEnc)
		if err != nil {
			return nil, err
		}
	}
	var val interface{} = nil
	if msgSer.ValueEnc != nil {
		val, err = valSerde.Decode(msgSer.ValueEnc)
		if err != nil {
			return nil, err
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
	KeySerde Serde
	ValSerde Serde
}

func (s MessageMsgpSerde) GetKeySerde() Serde {
	return s.KeySerde
}

func (s MessageMsgpSerde) GetValSerde() Serde {
	return s.ValSerde
}

func (s MessageMsgpSerde) Encode(value interface{}) ([]byte, error) {
	msg, err := convertToMsgSer(value, s.KeySerde, s.ValSerde)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return msg.MarshalMsg(nil)
}

func (s MessageMsgpSerde) Decode(value []byte) (interface{}, error) {
	msgSer := MessageSerialized{}
	_, err := msgSer.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return decodeToMsg(&msgSer, s.KeySerde, s.ValSerde)
}

type MessageJSONSerde struct {
	KeySerde Serde
	ValSerde Serde
}

func (s MessageJSONSerde) GetKeySerde() Serde {
	return s.KeySerde
}

func (s MessageJSONSerde) GetValSerde() Serde {
	return s.ValSerde
}

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

func (s MessageJSONSerde) Decode(value []byte) (interface{}, error) {
	msgSer := MessageSerialized{}
	if err := json.Unmarshal(value, &msgSer); err != nil {
		return nil, err
	}
	return decodeToMsg(&msgSer, s.KeySerde, s.ValSerde)
}

func GetMsgSerde(serdeFormat SerdeFormat, keySerde Serde, valSerde Serde) (MessageSerde, error) {
	if serdeFormat == JSON {
		return MessageJSONSerde{
			KeySerde: keySerde,
			ValSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return MessageMsgpSerde{
			KeySerde: keySerde,
			ValSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
