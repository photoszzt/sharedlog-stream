//go:generate msgp
//msgp:ignore KeyAndWindowStartTs KeyAndWindowStartTsJSONSerde KeyAndWindowStartTsMsgpSerde
package commtypes

import "encoding/json"

type KeyAndWindowStartTs struct {
	Key           interface{}
	WindowStartTs int64
}
type KeyAndWindowStartTsSerialized struct {
	KeySerialized []byte `msg:"k" json:"k"`
	WindowStartTs int64  `msg:"ts" json:"ts"`
}

type KeyAndWindowStartTsJSONSerde struct {
	KeyJSONSerde Serde
}

func castToKeyAndWindowStartTs(value interface{}) *KeyAndWindowStartTs {
	v, ok := value.(*KeyAndWindowStartTs)
	if !ok {
		vtmp := value.(KeyAndWindowStartTs)
		v = &vtmp
	}
	return v
}

func (s KeyAndWindowStartTsJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := castToKeyAndWindowStartTs(value)
	kenc, err := s.KeyJSONSerde.Encode(val.Key)
	if err != nil {
		return nil, err
	}
	kw := &KeyAndWindowStartTsSerialized{
		KeySerialized: kenc,
		WindowStartTs: val.WindowStartTs,
	}
	return json.Marshal(kw)
}

func (s KeyAndWindowStartTsJSONSerde) Decode(value []byte) (interface{}, error) {
	val := KeyAndWindowStartTsSerialized{}
	if err := json.Unmarshal(value, &val); err != nil {
		return nil, err
	}
	k, err := s.KeyJSONSerde.Decode(val.KeySerialized)
	if err != nil {
		return nil, err
	}
	return KeyAndWindowStartTs{
		Key:           k,
		WindowStartTs: val.WindowStartTs,
	}, nil
}

type KeyAndWindowStartTsMsgpSerde struct {
	KeyMsgpSerde Serde
}

func (s KeyAndWindowStartTsMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := castToKeyAndWindowStartTs(value)
	kenc, err := s.KeyMsgpSerde.Encode(val.Key)
	if err != nil {
		return nil, err
	}
	kw := &KeyAndWindowStartTsSerialized{
		KeySerialized: kenc,
		WindowStartTs: val.WindowStartTs,
	}
	return kw.MarshalMsg(nil)
}

func (s KeyAndWindowStartTsMsgpSerde) Decode(value []byte) (interface{}, error) {
	val := KeyAndWindowStartTsSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	k, err := s.KeyMsgpSerde.Decode(val.KeySerialized)
	if err != nil {
		return nil, err
	}
	return KeyAndWindowStartTs{
		Key:           k,
		WindowStartTs: val.WindowStartTs,
	}, nil
}
