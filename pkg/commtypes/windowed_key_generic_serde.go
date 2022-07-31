package commtypes

import "encoding/json"

type WindowedKeyJSONSerdeG struct {
	KeyJSONSerde    Serde
	WindowJSONSerde Serde
}

var _ = SerdeG[WindowedKey](WindowedKeyJSONSerdeG{})

func (s WindowedKeyJSONSerdeG) Encode(value WindowedKey) ([]byte, error) {
	wk, err := winKeyToWindowedKeySer(value, s.KeyJSONSerde, s.WindowJSONSerde)
	if err != nil {
		return nil, err
	}
	if wk == nil {
		return nil, nil
	}
	return json.Marshal(&wk)
}

func (s WindowedKeyJSONSerdeG) Decode(value []byte) (WindowedKey, error) {
	wk := WindowedKeySerialized{}
	if err := json.Unmarshal(value, &wk); err != nil {
		return WindowedKey{}, err
	}
	return winKeySerToWindowedKey(&wk, s.KeyJSONSerde, s.WindowJSONSerde)
}

type WindowedKeyMsgpSerdeG struct {
	KeyMsgpSerde    Serde
	WindowMsgpSerde Serde
}

var _ = SerdeG[WindowedKey](WindowedKeyMsgpSerdeG{})

func (s WindowedKeyMsgpSerdeG) Encode(value WindowedKey) ([]byte, error) {
	wk, err := winKeyToWindowedKeySer(value, s.KeyMsgpSerde, s.WindowMsgpSerde)
	if err != nil {
		return nil, err
	}
	if wk == nil {
		return nil, nil
	}
	return wk.MarshalMsg(nil)
}

func (s WindowedKeyMsgpSerdeG) Decode(value []byte) (WindowedKey, error) {
	wk := WindowedKeySerialized{}
	_, err := wk.UnmarshalMsg(value)
	if err != nil {
		return WindowedKey{}, err
	}
	return winKeySerToWindowedKey(&wk, s.KeyMsgpSerde, s.WindowMsgpSerde)
}