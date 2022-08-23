package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/utils"

	"4d63.com/optional"
)

type ValueTimestampJSONSerdeG struct {
	ValJSONSerde Serde
}

var _ = SerdeG[ValueTimestamp](ValueTimestampJSONSerdeG{})

func (s ValueTimestampJSONSerdeG) Encode(value ValueTimestamp) ([]byte, error) {
	vs, err := valTsToValueTsSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return json.Marshal(vs)
}

func (s ValueTimestampJSONSerdeG) Decode(value []byte) (ValueTimestamp, error) {
	if value == nil {
		return ValueTimestamp{}, nil
	}
	vs := ValueTimestampSerialized{}
	if err := json.Unmarshal(value, &vs); err != nil {
		return ValueTimestamp{}, err
	}
	return valTsSerToValueTs(&vs, s.ValJSONSerde)
}

type ValueTimestampMsgpSerdeG struct {
	ValMsgpSerde Serde
}

func (s ValueTimestampMsgpSerdeG) Encode(value ValueTimestamp) ([]byte, error) {
	vs, err := valTsToValueTsSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return vs.MarshalMsg(nil)
}

func (s ValueTimestampMsgpSerdeG) Decode(value []byte) (ValueTimestamp, error) {
	if value == nil {
		return ValueTimestamp{}, nil
	}
	vs := ValueTimestampSerialized{}
	_, err := vs.UnmarshalMsg(value)
	if err != nil {
		return ValueTimestamp{}, err
	}
	return valTsSerToValueTs(&vs, s.ValMsgpSerde)
}

func GetValueTsSerdeG(serdeFormat SerdeFormat, valSerde Serde) (SerdeG[ValueTimestamp], error) {
	if serdeFormat == JSON {
		return ValueTimestampJSONSerdeG{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ValueTimestampMsgpSerdeG{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}

func CreateValueTimestampOptional[V any](val optional.Optional[V], ts int64) optional.Optional[ValueTimestamp] {
	v, hasV := val.Get()
	if hasV {
		return optional.Of(ValueTimestamp{
			Value:     v,
			Timestamp: ts,
		})
	} else {
		return optional.Empty[ValueTimestamp]()
	}
}

func CreateValueTimestampOptionalWithIntrVal(val interface{}, ts int64) optional.Optional[ValueTimestamp] {
	if utils.IsNil(val) {
		return optional.Empty[ValueTimestamp]()
	} else {
		return optional.Of(ValueTimestamp{
			Value:     val,
			Timestamp: ts,
		})
	}
}

func CreateValueTimestampGOptionalWithIntrVal[V any](val interface{}, ts int64) optional.Optional[ValueTimestampG[V]] {
	if utils.IsNil(val) {
		return optional.Empty[ValueTimestampG[V]]()
	} else {
		return optional.Of(ValueTimestampG[V]{
			Value:     val.(V),
			Timestamp: ts,
		})
	}
}

func valTsGToValueTsSer[V any](value ValueTimestampG[V], valSerde SerdeG[V]) (*ValueTimestampSerialized, error) {
	enc, err := valSerde.Encode(value.Value)
	if err != nil {
		return nil, err
	}
	return &ValueTimestampSerialized{
		Timestamp:       value.Timestamp,
		ValueSerialized: enc,
	}, nil
}

func valTsSerToValueTsG[V any](vtsSer *ValueTimestampSerialized, valSerde SerdeG[V]) (ValueTimestampG[V], error) {
	var v V
	var err error
	if vtsSer.ValueSerialized != nil {
		v, err = valSerde.Decode(vtsSer.ValueSerialized)
		if err != nil {
			return ValueTimestampG[V]{}, err
		}
	}
	return ValueTimestampG[V]{
		Timestamp: vtsSer.Timestamp,
		Value:     v,
	}, nil
}

type ValueTimestampG[V any] struct {
	Value     V
	Timestamp int64
}

type ValueTimestampGSize[V any] struct {
	ValSizeFunc func(V) int64
}

func (s ValueTimestampGSize[V]) SizeOfValueTimestamp(v ValueTimestampG[V]) int64 {
	return 8 + s.ValSizeFunc(v.Value)
}

type ValueTimestampGJSONSerdeG[V any] struct {
	ValJSONSerde SerdeG[V]
}

func (s ValueTimestampGJSONSerdeG[V]) Encode(value ValueTimestampG[V]) ([]byte, error) {
	vs, err := valTsGToValueTsSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return json.Marshal(vs)
}

func (s ValueTimestampGJSONSerdeG[V]) Decode(value []byte) (ValueTimestampG[V], error) {
	if value == nil {
		return ValueTimestampG[V]{}, nil
	}
	vs := ValueTimestampSerialized{}
	if err := json.Unmarshal(value, &vs); err != nil {
		return ValueTimestampG[V]{}, err
	}
	return valTsSerToValueTsG(&vs, s.ValJSONSerde)
}

type ValueTimestampGMsgpSerdeG[V any] struct {
	ValMsgpSerde SerdeG[V]
}

func (s ValueTimestampGMsgpSerdeG[V]) Encode(value ValueTimestampG[V]) ([]byte, error) {
	vs, err := valTsGToValueTsSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return vs.MarshalMsg(nil)
}

func (s ValueTimestampGMsgpSerdeG[V]) Decode(value []byte) (ValueTimestampG[V], error) {
	if value == nil {
		return ValueTimestampG[V]{}, nil
	}
	vs := ValueTimestampSerialized{}
	_, err := vs.UnmarshalMsg(value)
	if err != nil {
		return ValueTimestampG[V]{}, err
	}
	return valTsSerToValueTsG(&vs, s.ValMsgpSerde)
}

func CreateValueTimestampGOptional[V any](val optional.Optional[V], ts int64) optional.Optional[ValueTimestampG[V]] {
	v, hasV := val.Get()
	if hasV {
		return optional.Of(ValueTimestampG[V]{
			Value:     v,
			Timestamp: ts,
		})
	} else {
		return optional.Empty[ValueTimestampG[V]]()
	}
}

func GetValueTsGSerdeG[V any](serdeFormat SerdeFormat, valSerde SerdeG[V]) (SerdeG[ValueTimestampG[V]], error) {
	if serdeFormat == JSON {
		return ValueTimestampGJSONSerdeG[V]{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ValueTimestampGMsgpSerdeG[V]{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
