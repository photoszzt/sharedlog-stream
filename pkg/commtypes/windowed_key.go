//go:generate msgp
//msgp:ignore WindowedKey WindowedKeyJSONSerde WindowedKeyMsgpSerde
package commtypes

import "encoding/json"

type WindowedKey struct {
	Key    interface{}
	Window Window
}

type WindowedKeySerialized struct {
	KeySerialized    []byte `json:"ks" msg:"ks"`
	WindowSerialized []byte `json:"ws" msg:"ws"`
}

type WindowedKeyJSONSerde struct {
	KeyJSONSerde    Serde
	WindowJSONSerde Serde
}

func (s WindowedKeyJSONSerde) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(*WindowedKey)
	if !ok {
		vtmp := value.(WindowedKey)
		v = &vtmp
	}
	kenc, err := s.KeyJSONSerde.Encode(v.Key)
	if err != nil {
		return nil, err
	}
	wenc, err := s.WindowJSONSerde.Encode(v.Window)
	if err != nil {
		return nil, err
	}
	wk := WindowedKeySerialized{
		KeySerialized:    kenc,
		WindowSerialized: wenc,
	}
	return json.Marshal(&wk)
}

func (s WindowedKeyJSONSerde) Decode(value []byte) (interface{}, error) {
	wk := WindowedKeySerialized{}
	if err := json.Unmarshal(value, &wk); err != nil {
		return nil, err
	}
	k, err := s.KeyJSONSerde.Decode(wk.KeySerialized)
	if err != nil {
		return nil, err
	}
	w, err := s.WindowJSONSerde.Decode(wk.WindowSerialized)
	if err != nil {
		return nil, err
	}
	return WindowedKey{
		Key:    k,
		Window: w.(Window),
	}, nil
}

type WindowedKeyMsgpSerde struct {
	KeyMsgpSerde    Serde
	WindowMsgpSerde Serde
}

func (s WindowedKeyMsgpSerde) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(*WindowedKey)
	if !ok {
		vtmp := value.(WindowedKey)
		v = &vtmp
	}
	kenc, err := s.KeyMsgpSerde.Encode(v.Key)
	if err != nil {
		return nil, err
	}
	wenc, err := s.WindowMsgpSerde.Encode(v.Window)
	if err != nil {
		return nil, err
	}
	wk := WindowedKeySerialized{
		KeySerialized:    kenc,
		WindowSerialized: wenc,
	}
	return wk.MarshalMsg(nil)
}

func (s WindowedKeyMsgpSerde) Decode(value []byte) (interface{}, error) {
	wk := WindowedKeySerialized{}
	_, err := wk.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	k, err := s.KeyMsgpSerde.Decode(wk.KeySerialized)
	if err != nil {
		return nil, err
	}
	w, err := s.WindowMsgpSerde.Decode(wk.WindowSerialized)
	if err != nil {
		return nil, err
	}
	return WindowedKey{
		Key:    k,
		Window: w.(Window),
	}, nil
}
