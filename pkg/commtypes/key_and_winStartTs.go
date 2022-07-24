//go:generate msgp
//msgp:ignore KeyAndWindowStartTs KeyAndWindowStartTsJSONSerde KeyAndWindowStartTsMsgpSerde
package commtypes

import (
	"encoding/json"
	"fmt"
)

type KeyAndWindowStartTs struct {
	Key           interface{}
	WindowStartTs int64
}

type KeyAndWindowStartTsG[K any] struct {
	Key           K
	WindowStartTs int64
}

func (kwTs KeyAndWindowStartTs) String() string {
	return fmt.Sprintf("KeyAndWindowStartTs: {Key: %v, WindowStartTs: %d}", kwTs.Key, kwTs.WindowStartTs)
}

func (kwTs KeyAndWindowStartTsG[K]) String() string {
	return fmt.Sprintf("KeyAndWindowStartTs: {Key: %v, WindowStartTs: %d}", kwTs.Key, kwTs.WindowStartTs)
}

type KeyAndWindowStartTsSerialized struct {
	KeySerialized []byte `msg:"k" json:"k"`
	WindowStartTs int64  `msg:"ts" json:"ts"`
}

type KeyAndWindowStartTsJSONSerde struct {
	KeyJSONSerde Serde
}

var _ Serde = KeyAndWindowStartTsJSONSerde{}

type KeyAndWindowStartTsJSONSerdeG[K any] struct {
	KeyJSONSerde SerdeG[K]
}

var _ SerdeG[KeyAndWindowStartTsG[int]] = KeyAndWindowStartTsJSONSerdeG[int]{}

func convertToKeyAndWindowStartTsSer(value interface{}, keySerde Serde) (*KeyAndWindowStartTsSerialized, error) {
	if value == nil {
		return nil, nil
	}
	val, ok := value.(*KeyAndWindowStartTs)
	if !ok {
		vtmp := value.(KeyAndWindowStartTs)
		val = &vtmp
	}
	if val == nil {
		return nil, nil
	}
	kenc, err := keySerde.Encode(val.Key)
	if err != nil {
		return nil, err
	}
	kw := &KeyAndWindowStartTsSerialized{
		KeySerialized: kenc,
		WindowStartTs: val.WindowStartTs,
	}
	return kw, nil
}

func kwsToKwsSer[K any](value KeyAndWindowStartTsG[K], keySerde SerdeG[K]) (*KeyAndWindowStartTsSerialized, error) {
	kenc, err := keySerde.Encode(value.Key)
	if err != nil {
		return nil, err
	}
	kw := &KeyAndWindowStartTsSerialized{
		KeySerialized: kenc,
		WindowStartTs: value.WindowStartTs,
	}
	return kw, nil
}

func decodeToKeyAndWindowStartTs(kwSer *KeyAndWindowStartTsSerialized, keySerde Serde) (interface{}, error) {
	var err error
	var k interface{}
	if kwSer.KeySerialized != nil {
		k, err = keySerde.Decode(kwSer.KeySerialized)
		if err != nil {
			return nil, err
		}
	}
	return KeyAndWindowStartTs{
		Key:           k,
		WindowStartTs: kwSer.WindowStartTs,
	}, nil
}

func serToKeyAndWindowStartTs[K any](kwSer *KeyAndWindowStartTsSerialized, keySerde SerdeG[K]) (KeyAndWindowStartTsG[K], error) {
	var err error
	var k K
	if kwSer.KeySerialized != nil {
		k, err = keySerde.Decode(kwSer.KeySerialized)
		if err != nil {
			return KeyAndWindowStartTsG[K]{}, err
		}
	}
	return KeyAndWindowStartTsG[K]{
		Key:           k,
		WindowStartTs: kwSer.WindowStartTs,
	}, nil
}

func (s KeyAndWindowStartTsJSONSerde) Encode(value interface{}) ([]byte, error) {
	kw, err := convertToKeyAndWindowStartTsSer(value, s.KeyJSONSerde)
	if err != nil {
		return nil, err
	}
	if kw == nil {
		return nil, nil
	}
	return json.Marshal(kw)
}

func (s KeyAndWindowStartTsJSONSerde) Decode(value []byte) (interface{}, error) {
	val := KeyAndWindowStartTsSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return nil, err
	}
	return decodeToKeyAndWindowStartTs(&val, s.KeyJSONSerde)
}

func (s KeyAndWindowStartTsJSONSerdeG[K]) Encode(value KeyAndWindowStartTsG[K]) ([]byte, error) {
	kw, err := kwsToKwsSer(value, s.KeyJSONSerde)
	if err != nil {
		return nil, err
	}
	if kw == nil {
		return nil, nil
	}
	return json.Marshal(kw)
}

func (s KeyAndWindowStartTsJSONSerdeG[K]) Decode(value []byte) (KeyAndWindowStartTsG[K], error) {
	val := KeyAndWindowStartTsSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return KeyAndWindowStartTsG[K]{}, err
	}
	return serToKeyAndWindowStartTs(&val, s.KeyJSONSerde)
}

type KeyAndWindowStartTsMsgpSerde struct {
	KeyMsgpSerde Serde
}

var _ = Serde(KeyAndWindowStartTsMsgpSerde{})

func (s KeyAndWindowStartTsMsgpSerde) Encode(value interface{}) ([]byte, error) {
	kw, err := convertToKeyAndWindowStartTsSer(value, s.KeyMsgpSerde)
	if err != nil {
		return nil, err
	}
	if kw == nil {
		return nil, nil
	}
	return kw.MarshalMsg(nil)
}

func (s KeyAndWindowStartTsMsgpSerde) Decode(value []byte) (interface{}, error) {
	val := KeyAndWindowStartTsSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return decodeToKeyAndWindowStartTs(&val, s.KeyMsgpSerde)
}

type KeyAndWindowStartTsMsgpSerdeG[K any] struct {
	KeyMsgpSerde SerdeG[K]
}

var _ = SerdeG[KeyAndWindowStartTsG[int]](KeyAndWindowStartTsMsgpSerdeG[int]{})

func (s KeyAndWindowStartTsMsgpSerdeG[K]) Encode(value KeyAndWindowStartTsG[K]) ([]byte, error) {
	kw, err := kwsToKwsSer(value, s.KeyMsgpSerde)
	if err != nil {
		return nil, err
	}
	if kw == nil {
		return nil, nil
	}
	return kw.MarshalMsg(nil)
}

func (s KeyAndWindowStartTsMsgpSerdeG[K]) Decode(value []byte) (KeyAndWindowStartTsG[K], error) {
	val := KeyAndWindowStartTsSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return KeyAndWindowStartTsG[K]{}, err
	}
	return serToKeyAndWindowStartTs(&val, s.KeyMsgpSerde)
}
