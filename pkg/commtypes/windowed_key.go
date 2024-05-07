//go:generate msgp
//msgp:ignore WindowedKey WindowedKeyJSONSerde WindowedKeyMsgpSerde
package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/utils"
)

type WindowedKey struct {
	Key    interface{}
	Window Window
}

type WindowedKeySerialized struct {
	KeySerialized    []byte `json:"ks" msg:"ks"`
	WindowSerialized []byte `json:"ws" msg:"ws"`
}

func convertToWindowedKeySer(value interface{}, keySerde Serde, windowSerde Serde) (*WindowedKeySerialized, error) {
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

func winKeyToWindowedKeySer(value WindowedKey, keySerde Serde, windowSerde Serde) (*WindowedKeySerialized, error) {
	var err error
	var kenc, wenc []byte = nil, nil
	if !utils.IsNil(value.Key) {
		kenc, err = keySerde.Encode(value.Key)
		if err != nil {
			return nil, err
		}
	}
	if !utils.IsNil(value.Window) {
		wenc, err = windowSerde.Encode(value.Window)
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

func winKeySerToWindowedKey(wkSer *WindowedKeySerialized, keySerde Serde, windowSerde Serde) (WindowedKey, error) {
	var err error
	var k, w interface{}
	if wkSer.KeySerialized != nil {
		k, err = keySerde.Decode(wkSer.KeySerialized)
		if err != nil {
			return WindowedKey{}, err
		}
	}
	if wkSer.WindowSerialized != nil {
		w, err = windowSerde.Decode(wkSer.WindowSerialized)
		if err != nil {
			return WindowedKey{}, err
		}
	}
	return WindowedKey{
		Key:    k,
		Window: w.(Window),
	}, nil
}

type WindowedKeyJSONSerde struct {
	DefaultJSONSerde
	KeyJSONSerde    Serde
	WindowJSONSerde Serde
}

var _ = Serde(WindowedKeyJSONSerde{})

func (s WindowedKeyJSONSerde) Encode(value interface{}) ([]byte, error) {
	wk, err := convertToWindowedKeySer(value, s.KeyJSONSerde, s.WindowJSONSerde)
	defer func() {
		if wk != nil {
			if s.KeyJSONSerde.UsedBufferPool() && wk.KeySerialized != nil {
				PushBuffer(&wk.KeySerialized)
			}
			if s.WindowJSONSerde.UsedBufferPool() && wk.WindowSerialized != nil {
				PushBuffer(&wk.WindowSerialized)
			}
		}
	}()
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
	DefaultMsgpSerde
	KeyMsgpSerde    Serde
	WindowMsgpSerde Serde
}

var _ = Serde(WindowedKeyMsgpSerde{})

func (s WindowedKeyMsgpSerde) Encode(value interface{}) ([]byte, error) {
	wk, err := convertToWindowedKeySer(value, s.KeyMsgpSerde, s.WindowMsgpSerde)
	defer func() {
		if wk != nil {
			if s.KeyMsgpSerde.UsedBufferPool() && wk.KeySerialized != nil {
				PushBuffer(&wk.KeySerialized)
			}
			if s.WindowMsgpSerde.UsedBufferPool() && wk.WindowSerialized != nil {
				PushBuffer(&wk.WindowSerialized)
			}
		}
	}()
	if err != nil {
		return nil, err
	}
	if wk == nil {
		return nil, nil
	}
	b := PopBuffer()
	buf := *b
	return wk.MarshalMsg(buf[:0])
}

func (s WindowedKeyMsgpSerde) Decode(value []byte) (interface{}, error) {
	wk := WindowedKeySerialized{}
	_, err := wk.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return decodeToWindowedKey(&wk, s.KeyMsgpSerde, s.WindowMsgpSerde)
}
