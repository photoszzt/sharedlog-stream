package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
)

type KeyAndWindowStartTsJSONSerdeG[K any] struct {
	KeyJSONSerde SerdeG[K]
}

type KeyAndWindowStartTsG[K any] struct {
	Key           K
	WindowStartTs int64
}

type KeyAndWindowStartTsGSize[K any] struct {
	KeySizeFunc func(K) int64
}

func (s KeyAndWindowStartTsGSize[K]) SizeOfKeyAndWindowStartTs(v KeyAndWindowStartTsG[K]) int64 {
	return 8 + s.KeySizeFunc(v.Key)
}

func (kwTs KeyAndWindowStartTsG[K]) String() string {
	return fmt.Sprintf("KeyAndWindowStartTs: {Key: %v, WindowStartTs: %d}", kwTs.Key, kwTs.WindowStartTs)
}

var _ SerdeG[KeyAndWindowStartTsG[int]] = KeyAndWindowStartTsJSONSerdeG[int]{}

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
	b := PopBuffer()
	buf := *b
	ret, err := kw.MarshalMsg(buf[:0])
	PushBuffer(&kw.KeySerialized)
	return ret, err
}

func (s KeyAndWindowStartTsMsgpSerdeG[K]) Decode(value []byte) (KeyAndWindowStartTsG[K], error) {
	val := KeyAndWindowStartTsSerialized{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return KeyAndWindowStartTsG[K]{}, err
	}
	return serToKeyAndWindowStartTs(&val, s.KeyMsgpSerde)
}

func GetKeyAndWindowStartTsSerdeG[K any](serdeFormat SerdeFormat, keySerde SerdeG[K]) (SerdeG[KeyAndWindowStartTsG[K]], error) {
	if serdeFormat == JSON {
		return KeyAndWindowStartTsJSONSerdeG[K]{
			KeyJSONSerde: keySerde,
		}, nil
	} else if serdeFormat == MSGP {
		return KeyAndWindowStartTsMsgpSerdeG[K]{
			KeyMsgpSerde: keySerde,
		}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
