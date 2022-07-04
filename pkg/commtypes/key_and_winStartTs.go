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

func (kwTs KeyAndWindowStartTs) String() string {
	return fmt.Sprintf("KeyAndWindowStartTs: {Key: %v, WindowStartTs: %d}", kwTs.Key, kwTs.WindowStartTs)
}

type KeyAndWindowStartTsSerialized struct {
	KeySerialized []byte `msg:"k" json:"k"`
	WindowStartTs int64  `msg:"ts" json:"ts"`
}

type KeyAndWindowStartTsJSONSerde struct {
	KeyJSONSerde Serde
}

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

type KeyAndWindowStartTsMsgpSerde struct {
	KeyMsgpSerde Serde
}

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
