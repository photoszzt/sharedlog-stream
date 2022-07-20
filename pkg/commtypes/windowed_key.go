//go:generate msgp
//msgp:ignore WindowedKey WindowedKeyJSONSerde WindowedKeyMsgpSerde
package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/utils"
)

type WindowedKey[K any] struct {
	Key    K
	Window Window
}

type WindowedKeySerialized struct {
	KeySerialized    []byte `json:"ks" msg:"ks"`
	WindowSerialized []byte `json:"ws" msg:"ws"`
}

type WindowedKeyJSONSerde[K any] struct {
	KeyJSONSerde    Serde[K]
	WindowJSONSerde Serde[Window]
}

func castToWindowedKey(value interface{}) *WindowedKey {
	v, ok := value.(*WindowedKey)
	if !ok {
		vtmp := value.(WindowedKey)
		v = &vtmp
	}
	return v
}

func convertToWindowedKeySer[K any](value interface{}, keySerde Serde[K], windowSerde Serde[Window]) (*WindowedKeySerialized, error) {
	if value == nil {
		return nil, nil
	}
	v, ok := value.(*WindowedKey)
	if !ok {
		vtmp := value.(WindowedKey)
		v = &vtmp
	}
	if v == nil {
		return nil, nil
	}
	var err error
	var kenc, wenc []byte = nil, nil
	if !utils.IsNil(v.Key) {
		kenc, err = keySerde.Encode(v.Key)
		if err != nil {
			return nil, err
		}
	}
	if !utils.IsNil(v.Window) {
		wenc, err = windowSerde.Encode(v.Window)
		if err != nil {
			return nil, err
		}
	}
	if kenc == nil && wenc == nil {
		return nil, nil
	}
	wk := &WindowedKeySerialized{
		KeySerialized:    kenc,
		WindowSerialized: wenc,
	}
	return wk, nil
}

func decodeToWindowedKey(wkSer *WindowedKeySerialized, keySerde Serde, windowSerde Serde) (interface{}, error) {
	var err error
	var k, w interface{}
	if wkSer.KeySerialized != nil {
		k, err = keySerde.Decode(wkSer.KeySerialized)
		if err != nil {
			return nil, err
		}
	}
	if wkSer.WindowSerialized != nil {
		w, err = windowSerde.Decode(wkSer.WindowSerialized)
		if err != nil {
			return nil, err
		}
	}
	return WindowedKey{
		Key:    k,
		Window: w.(Window),
	}, nil
}

func (s WindowedKeyJSONSerde) Encode(value interface{}) ([]byte, error) {
	wk, err := convertToWindowedKeySer(value, s.KeyJSONSerde, s.WindowJSONSerde)
	if err != nil {
		return nil, err
	}
	if wk == nil {
		return nil, nil
	}
	return json.Marshal(&wk)
}

func (s WindowedKeyJSONSerde) Decode(value []byte) (interface{}, error) {
	wk := WindowedKeySerialized{}
	if err := json.Unmarshal(value, &wk); err != nil {
		return nil, err
	}
	return decodeToWindowedKey(&wk, s.KeyJSONSerde, s.WindowJSONSerde)
}

type WindowedKeyMsgpSerde struct {
	KeyMsgpSerde    Serde
	WindowMsgpSerde Serde
}

func (s WindowedKeyMsgpSerde) Encode(value interface{}) ([]byte, error) {
	wk, err := convertToWindowedKeySer(value, s.KeyMsgpSerde, s.WindowMsgpSerde)
	if err != nil {
		return nil, err
	}
	if wk == nil {
		return nil, nil
	}
	return wk.MarshalMsg(nil)
}

func (s WindowedKeyMsgpSerde) Decode(value []byte) (interface{}, error) {
	wk := WindowedKeySerialized{}
	_, err := wk.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return decodeToWindowedKey(&wk, s.KeyMsgpSerde, s.WindowMsgpSerde)
}
