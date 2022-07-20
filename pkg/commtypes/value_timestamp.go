//go:generate msgp
//msgp:ignore ValueTimestampJSONSerde ValueTimestampMsgpSerde ValueTimestamp
package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/utils"

	"4d63.com/optional"
)

type ValueTimestamp[V any] struct {
	Value     V
	Timestamp int64
}

var _ = fmt.Stringer(ValueTimestamp[int]{})

func (vts ValueTimestamp[V]) String() string {
	return fmt.Sprintf("ValueTs: {Value: %v, Ts: %d}", vts.Value, vts.Timestamp)
}

func CreateValueTimestamp[V any](val optional.Optional[V], ts int64) *ValueTimestamp[V] {
	if v, ok := val.Get(); ok {
		return &ValueTimestamp[V]{
			Value:     v,
			Timestamp: ts,
		}
	} else {
		return nil
	}
}

type ValueTimestampSerialized struct {
	ValueSerialized []byte `json:"vs,omitempty" msg:"vs,omitempty"`
	Timestamp       int64  `json:"ts,omitempty" msg:"ts,omitempty"`
}

func (s *ValueTimestamp[V]) ExtractEventTime() (int64, error) {
	return s.Timestamp, nil
}

func GetValOrNil[V any](valTs *ValueTimestamp[V]) interface{} {
	if valTs == nil {
		return nil
	} else {
		return valTs.Value
	}
}

var _ = EventTimeExtractor(&ValueTimestamp[int]{})

type ValueTimestampJSONSerde[V any] struct {
	ValJSONSerde Serde[V]
}

var _ = Serde[ValueTimestamp[int]](ValueTimestampJSONSerde[int]{})

func convertToValueTsSer[V any](value ValueTimestamp[V], valSerde Serde[V]) (*ValueTimestampSerialized, error) {
	v := &value
	var enc []byte
	var err error
	if !utils.IsNil(v.Value) {
		enc, err = valSerde.Encode(v.Value)
		if err != nil {
			return nil, err
		}
	}
	return &ValueTimestampSerialized{
		Timestamp:       v.Timestamp,
		ValueSerialized: enc,
	}, nil
}

func (s ValueTimestampJSONSerde[V]) Encode(value ValueTimestamp[V]) ([]byte, error) {
	vs, err := convertToValueTsSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return json.Marshal(vs)
}

func decodeToValueTs[V any](vtsSer *ValueTimestampSerialized, valSerde Serde[V]) (ValueTimestamp[V], error) {
	var v V
	var err error
	if vtsSer.ValueSerialized != nil {
		v, err = valSerde.Decode(vtsSer.ValueSerialized)
		if err != nil {
			return ValueTimestamp[V]{}, err
		}
	}
	return ValueTimestamp[V]{
		Timestamp: vtsSer.Timestamp,
		Value:     v,
	}, nil
}

func (s ValueTimestampJSONSerde[V]) Decode(value []byte) (ValueTimestamp[V], error) {
	if value == nil {
		return ValueTimestamp[V]{}, nil
	}
	vs := ValueTimestampSerialized{}
	if err := json.Unmarshal(value, &vs); err != nil {
		return ValueTimestamp[V]{}, err
	}
	return decodeToValueTs(&vs, s.ValJSONSerde)
}

type ValueTimestampMsgpSerde[V any] struct {
	ValMsgpSerde Serde[V]
}

var _ = Serde[ValueTimestamp[int]](ValueTimestampMsgpSerde[int]{})

func (s ValueTimestampMsgpSerde[V]) Encode(value ValueTimestamp[V]) ([]byte, error) {
	vs, err := convertToValueTsSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return vs.MarshalMsg(nil)
}

func (s ValueTimestampMsgpSerde[V]) Decode(value []byte) (ValueTimestamp[V], error) {
	if value == nil {
		return ValueTimestamp[V]{}, nil
	}
	vs := ValueTimestampSerialized{}
	_, err := vs.UnmarshalMsg(value)
	if err != nil {
		return ValueTimestamp[V]{}, err
	}
	return decodeToValueTs(&vs, s.ValMsgpSerde)
}

func GetValueTsSerde[V any](serdeFormat SerdeFormat, valSerde Serde[V]) (Serde[ValueTimestamp[V]], error) {
	if serdeFormat == JSON {
		return ValueTimestampJSONSerde[V]{
			ValJSONSerde: valSerde,
		}, nil
	} else if serdeFormat == MSGP {
		return ValueTimestampMsgpSerde[V]{
			ValMsgpSerde: valSerde,
		}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
