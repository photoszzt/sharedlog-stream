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

var _ = Serde[WindowedKey[int]](WindowedKeyJSONSerde[int]{})

func convertToWindowedKeySer[K any](value WindowedKey[K], keySerde Serde[K], windowSerde Serde[Window]) (*WindowedKeySerialized, error) {
	v := &value
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

func decodeToWindowedKey[K any](wkSer *WindowedKeySerialized, keySerde Serde[K], windowSerde Serde[Window]) (WindowedKey[K], error) {
	var err error
	var k K
	var w Window
	if wkSer.KeySerialized != nil {
		k, err = keySerde.Decode(wkSer.KeySerialized)
		if err != nil {
			return WindowedKey[K]{}, err
		}
	}
	if wkSer.WindowSerialized != nil {
		w, err = windowSerde.Decode(wkSer.WindowSerialized)
		if err != nil {
			return WindowedKey[K]{}, err
		}
	}
	return WindowedKey[K]{
		Key:    k,
		Window: w,
	}, nil
}

func (s WindowedKeyJSONSerde[K]) Encode(value WindowedKey[K]) ([]byte, error) {
	wk, err := convertToWindowedKeySer(value, s.KeyJSONSerde, s.WindowJSONSerde)
	if err != nil {
		return nil, err
	}
	if wk == nil {
		return nil, nil
	}
	return json.Marshal(&wk)
}

func (s WindowedKeyJSONSerde[K]) Decode(value []byte) (WindowedKey[K], error) {
	wk := WindowedKeySerialized{}
	if err := json.Unmarshal(value, &wk); err != nil {
		return WindowedKey[K]{}, err
	}
	return decodeToWindowedKey(&wk, s.KeyJSONSerde, s.WindowJSONSerde)
}

type WindowedKeyMsgpSerde[K any] struct {
	KeyMsgpSerde    Serde[K]
	WindowMsgpSerde Serde[Window]
}

var _ = Serde[WindowedKey[int]](WindowedKeyMsgpSerde[int]{})

func (s WindowedKeyMsgpSerde[K]) Encode(value WindowedKey[K]) ([]byte, error) {
	wk, err := convertToWindowedKeySer(value, s.KeyMsgpSerde, s.WindowMsgpSerde)
	if err != nil {
		return nil, err
	}
	if wk == nil {
		return nil, nil
	}
	return wk.MarshalMsg(nil)
}

func (s WindowedKeyMsgpSerde[K]) Decode(value []byte) (WindowedKey[K], error) {
	wk := WindowedKeySerialized{}
	_, err := wk.UnmarshalMsg(value)
	if err != nil {
		return WindowedKey[K]{}, err
	}
	return decodeToWindowedKey(&wk, s.KeyMsgpSerde, s.WindowMsgpSerde)
}
