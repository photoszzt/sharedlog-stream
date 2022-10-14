//go:generate msgp
//msgp:ignore MessageMsgpSerde MessageJSONSerde

package commtypes

import (
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/pkg/utils"
)

type MessageSerialized struct {
	KeyEnc      []byte `json:"key,omitempty" msg:"key,omitempty"`
	ValueEnc    []byte `json:"val,omitempty" msg:"val,omitempty"`
	InjTMs      int64  `msg:"injT" json:"injT"`
	TimestampMs int64  `msg:"ts,omitempty" json:"ts,omitempty"`
}

func (m *MessageSerialized) UpdateInjectTime(ts int64) {
	m.InjTMs = ts
}

func (m *MessageSerialized) ExtractInjectTimeMs() int64 {
	return m.InjTMs
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
		KeyEnc:      kenc,
		ValueEnc:    venc,
		InjTMs:      v.InjT,
		TimestampMs: v.Timestamp,
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
		InjT:      msgSer.InjTMs,
		Timestamp: msgSer.TimestampMs,
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
		fmt.Fprintf(os.Stderr, "[ERROR] fail to unmarshal this msg3: %v", string(value))
		return nil, fmt.Errorf("fail to unmarshal msg: %v", err)
	}
	return decodeToMsg(&msgSer, s.keySerde, s.valSerde)
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
