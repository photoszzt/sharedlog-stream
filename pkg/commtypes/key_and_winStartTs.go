//go:generate msgp
//msgp:ignore KeyAndWindowStartTs KeyAndWindowStartTsJSONSerde KeyAndWindowStartTsMsgpSerde
package commtypes

import (
	"encoding/json"
	"fmt"
)

type KeyAndWindowStartTs[K any] struct {
	Key           K
	WindowStartTs int64
}

func (kwTs KeyAndWindowStartTs[K]) String() string {
	return fmt.Sprintf("KeyAndWindowStartTs: {Key: %v, WindowStartTs: %d}", kwTs.Key, kwTs.WindowStartTs)
}

type KeyAndWindowStartTsSerialized struct {
	KeySerialized []byte `msg:"k" json:"k"`
	WindowStartTs int64  `msg:"ts" json:"ts"`
}

type KeyAndWindowStartTsJSONSerde[K any] struct {
	KeyJSONSerde Serde[K]
}

var _ = Serde[KeyAndWindowStartTs[int]](&KeyAndWindowStartTsJSONSerde[int]{})

func convertToKeyAndWindowStartTsSer[K any](value KeyAndWindowStartTs[K], keySerde Serde[K]) (*KeyAndWindowStartTsSerialized, error) {
	val := &value
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

func decodeToKeyAndWindowStartTs[K any](kwSer *KeyAndWindowStartTsSerialized, keySerde Serde[K]) (KeyAndWindowStartTs[K], error) {
	var err error
	var k K
	if kwSer.KeySerialized != nil {
		k, err = keySerde.Decode(kwSer.KeySerialized)
		if err != nil {
			return KeyAndWindowStartTs[K]{}, err
		}
	}
	return KeyAndWindowStartTs[K]{
		Key:           k,
		WindowStartTs: kwSer.WindowStartTs,
	}, nil
}

func (s KeyAndWindowStartTsJSONSerde[K]) Encode(value KeyAndWindowStartTs[K]) ([]byte, error) {
	kw, err := convertToKeyAndWindowStartTsSer(value, s.KeyJSONSerde)
	if err != nil {
		return nil, err
	}
	if kw == nil {
		return nil, nil
	}
	return json.Marshal(kw)
}

func (s KeyAndWindowStartTsJSONSerde[K]) Decode(value []byte) (KeyAndWindowStartTs[K], error) {
	val := KeyAndWindowStartTsSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return KeyAndWindowStartTs[K]{}, err
	}
	return decodeToKeyAndWindowStartTs(&val, s.KeyJSONSerde)
}

type KeyAndWindowStartTsMsgpSerde[K any] struct {
	KeyMsgpSerde Serde[K]
}

var _ = Serde[KeyAndWindowStartTs[int]](&KeyAndWindowStartTsMsgpSerde[int]{})

func (s KeyAndWindowStartTsMsgpSerde[K]) Encode(value KeyAndWindowStartTs[K]) ([]byte, error) {
	kw, err := convertToKeyAndWindowStartTsSer(value, s.KeyMsgpSerde)
	if err != nil {
		return nil, err
	}
	if kw == nil {
		return nil, nil
	}
	return kw.MarshalMsg(nil)
}

func (s KeyAndWindowStartTsMsgpSerde[K]) Decode(value []byte) (KeyAndWindowStartTs[K], error) {
	val := KeyAndWindowStartTsSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return KeyAndWindowStartTs[K]{}, err
	}
	return decodeToKeyAndWindowStartTs(&val, s.KeyMsgpSerde)
}
