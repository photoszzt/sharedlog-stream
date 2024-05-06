package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
)

type KeyValuePair[K, V any] struct {
	Key   K
	Value V
}

type KeyValuePairs[K, V any] []*KeyValuePair[K, V]

func KVPairToKVPairSer[K, V any](value *KeyValuePair[K, V], keySerde SerdeG[K], valSerde SerdeG[V]) (KeyValueSerrialized, error) {
	kenc, err := keySerde.Encode(value.Key)
	if err != nil {
		return KeyValueSerrialized{}, fmt.Errorf("fail to encode key: %v", err)
	}
	venc, err := valSerde.Encode(value.Value)
	if err != nil {
		return KeyValueSerrialized{}, fmt.Errorf("fail encode val: %v", err)
	}
	return KeyValueSerrialized{
		KeyEnc:   kenc,
		ValueEnc: venc,
	}, nil
}

func KVPairSerToKVPairG[K, V any](value KeyValueSerrialized, keySerde SerdeG[K], valSerde SerdeG[V]) (*KeyValuePair[K, V], error) {
	key, err := keySerde.Decode(value.KeyEnc)
	if err != nil {
		return nil, fmt.Errorf("fail to decode key: %v", err)
	}
	val, err := valSerde.Decode(value.ValueEnc)
	if err != nil {
		return nil, fmt.Errorf("fail to decode val: %v", err)
	}
	return &KeyValuePair[K, V]{
		Key:   key,
		Value: val,
	}, nil
}

type KeyValuePairJSONSerdeG[K, V any] struct {
	keySerde SerdeG[K]
	valSerde SerdeG[V]
}

func (s KeyValuePairJSONSerdeG[K, V]) Encode(v *KeyValuePair[K, V]) ([]byte, error) {
	kvser, err := KVPairToKVPairSer(v, s.keySerde, s.valSerde)
	if err != nil {
		return nil, err
	}
	return json.Marshal(kvser)
}

func (s KeyValuePairJSONSerdeG[K, V]) Decode(v []byte) (*KeyValuePair[K, V], error) {
	var kvser KeyValueSerrialized
	err := json.Unmarshal(v, &kvser)
	if err != nil {
		return nil, err
	}
	return KVPairSerToKVPairG(kvser, s.keySerde, s.valSerde)
}

type KeyValuePairMsgpSerdeG[K, V any] struct {
	keySerde SerdeG[K]
	valSerde SerdeG[V]
}

func (s *KeyValuePairMsgpSerdeG[K, V]) Encode(v *KeyValuePair[K, V]) ([]byte, error) {
	kvser, err := KVPairToKVPairSer(v, s.keySerde, s.valSerde)
	if err != nil {
		return nil, err
	}
	return kvser.MarshalMsg(nil)
}

func (s *KeyValuePairMsgpSerdeG[K, V]) Decode(v []byte) (*KeyValuePair[K, V], error) {
	var kvser KeyValueSerrialized
	_, err := kvser.UnmarshalMsg(v)
	if err != nil {
		return nil, err
	}
	return KVPairSerToKVPairG(kvser, s.keySerde, s.valSerde)
}

func GetKeyValuePairSerdeG[K, V any](format SerdeFormat, keySerde SerdeG[K], valSerde SerdeG[V]) (SerdeG[*KeyValuePair[K, V]], error) {
	switch format {
	case JSON:
		return KeyValuePairJSONSerdeG[K, V]{keySerde, valSerde}, nil
	case MSGP:
		return &KeyValuePairMsgpSerdeG[K, V]{keySerde, valSerde}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}

type KeyValuePairsJSONSerdeG[K, V any] struct {
	payloadEnc PayloadArrJSONSerdeG
	s          KeyValuePairJSONSerdeG[K, V]
}

func (s KeyValuePairsJSONSerdeG[K, V]) Encode(v KeyValuePairs[K, V]) ([]byte, error) {
	payloadArr := PayloadArr{
		Payloads: make([][]byte, 0, len(v)),
	}
	for _, kv := range v {
		enc, err := s.s.Encode(kv)
		if err != nil {
			return nil, err
		}
		payloadArr.Payloads = append(payloadArr.Payloads, enc)
	}
	return s.payloadEnc.Encode(payloadArr)
}

func (s KeyValuePairsJSONSerdeG[K, V]) Decode(v []byte) (KeyValuePairs[K, V], error) {
	payloadArr, err := s.payloadEnc.Decode(v)
	if err != nil {
		return KeyValuePairs[K, V]{}, err
	}
	kvps := make(KeyValuePairs[K, V], len(payloadArr.Payloads))
	for i, payload := range payloadArr.Payloads {
		kvp, err := s.s.Decode(payload)
		if err != nil {
			return KeyValuePairs[K, V]{}, err
		}
		kvps[i] = kvp
	}
	return kvps, nil
}

type KeyValuePairsMsgpSerdeG[K, V any] struct {
	payloadEnc PayloadArrMsgpSerdeG
	s          KeyValuePairMsgpSerdeG[K, V]
}

func (s KeyValuePairsMsgpSerdeG[K, V]) Encode(v KeyValuePairs[K, V]) ([]byte, error) {
	payloadArr := PayloadArr{
		Payloads: make([][]byte, 0, len(v)),
	}
	for _, kv := range v {
		enc, err := s.s.Encode(kv)
		if err != nil {
			return nil, err
		}
		payloadArr.Payloads = append(payloadArr.Payloads, enc)
	}
	return s.payloadEnc.Encode(payloadArr)
}

func (s KeyValuePairsMsgpSerdeG[K, V]) Decode(v []byte) (KeyValuePairs[K, V], error) {
	payloadArr, err := s.payloadEnc.Decode(v)
	if err != nil {
		return KeyValuePairs[K, V]{}, err
	}
	kvps := make(KeyValuePairs[K, V], len(payloadArr.Payloads))
	for i, payload := range payloadArr.Payloads {
		kvp, err := s.s.Decode(payload)
		if err != nil {
			return KeyValuePairs[K, V]{}, err
		}
		kvps[i] = kvp
	}
	return kvps, nil
}

func GetKeyValuePairsSerdeG[K, V any](format SerdeFormat, keySerde SerdeG[K], valSerde SerdeG[V]) (SerdeG[KeyValuePairs[K, V]], error) {
	switch format {
	case JSON:
		return KeyValuePairsJSONSerdeG[K, V]{
			payloadEnc: PayloadArrJSONSerdeG{},
			s: KeyValuePairJSONSerdeG[K, V]{
				keySerde: keySerde,
				valSerde: valSerde,
			},
		}, nil
	case MSGP:
		return KeyValuePairsMsgpSerdeG[K, V]{
			payloadEnc: PayloadArrMsgpSerdeG{},
			s: KeyValuePairMsgpSerdeG[K, V]{
				keySerde: keySerde,
				valSerde: valSerde,
			},
		}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
