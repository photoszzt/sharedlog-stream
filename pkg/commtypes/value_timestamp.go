//go:generate msgp
//msgp:ignore ValueTimestampJSONSerde ValueTimestampMsgpSerde ValueTimestamp
package commtypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/utils"
)

type ValueTimestamp struct {
	Value     interface{}
	Timestamp int64
}

var _ = fmt.Stringer(ValueTimestamp{})

func (vts ValueTimestamp) String() string {
	return fmt.Sprintf("ValueTs: {Value: %v, Ts: %d}", vts.Value, vts.Timestamp)
}

func CreateValueTimestamp(val interface{}, ts int64) *ValueTimestamp {
	if val == nil {
		return nil
	} else {
		return &ValueTimestamp{
			Value:     val,
			Timestamp: ts,
		}
	}
}

type ValueTimestampSerialized struct {
	ValueSerialized []byte `json:"vs,omitempty" msg:"vs,omitempty"`
	Timestamp       int64  `json:"ts,omitempty" msg:"ts,omitempty"`
}

func (s *ValueTimestamp) ExtractEventTime() (int64, error) {
	return s.Timestamp, nil
}

func GetValOrNil(valTs *ValueTimestamp) interface{} {
	if valTs == nil {
		return nil
	} else {
		return valTs.Value
	}
}

var _ = EventTimeExtractor(&ValueTimestamp{})

func CastToValTsPtr(value interface{}) *ValueTimestamp {
	v, ok := value.(*ValueTimestamp)
	if !ok {
		vtmp := value.(ValueTimestamp)
		v = &vtmp
	}
	return v
}

func convertToValueTsSer(value interface{}, valSerde Serde) (*ValueTimestampSerialized, error) {
	if value == nil {
		return nil, nil
	}
	v := CastToValTsPtr(value)
	if v == nil {
		return nil, nil
	}
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

func valTsToValueTsSer(value *ValueTimestamp, valSerde Serde) (*ValueTimestampSerialized, error) {
	var enc []byte
	var err error
	if !utils.IsNil(value.Value) {
		enc, err = valSerde.Encode(value.Value)
		if err != nil {
			return nil, err
		}
	}
	return &ValueTimestampSerialized{
		Timestamp:       value.Timestamp,
		ValueSerialized: enc,
	}, nil
}

func decodeToValueTs(vtsSer *ValueTimestampSerialized, valSerde Serde) (interface{}, error) {
	var v interface{}
	var err error
	if vtsSer.ValueSerialized != nil {
		v, err = valSerde.Decode(vtsSer.ValueSerialized)
		if err != nil {
			return nil, err
		}
	}
	return ValueTimestamp{
		Timestamp: vtsSer.Timestamp,
		Value:     v,
	}, nil
}

func valTsSerToValueTs(vtsSer *ValueTimestampSerialized, valSerde Serde) (*ValueTimestamp, error) {
	var v interface{}
	var err error
	if vtsSer.ValueSerialized != nil {
		v, err = valSerde.Decode(vtsSer.ValueSerialized)
		if err != nil {
			return nil, err
		}
	}
	return &ValueTimestamp{
		Timestamp: vtsSer.Timestamp,
		Value:     v,
	}, nil
}

type ValueTimestampJSONSerde struct {
	ValJSONSerde Serde
}

type ValueTimestampJSONSerdeG struct {
	ValJSONSerde Serde
}

var _ = Serde(ValueTimestampJSONSerde{})
var _ = SerdeG[*ValueTimestamp](ValueTimestampJSONSerdeG{})

func (s ValueTimestampJSONSerde) Encode(value interface{}) ([]byte, error) {
	vs, err := convertToValueTsSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return json.Marshal(vs)
}

func (s ValueTimestampJSONSerde) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	vs := ValueTimestampSerialized{}
	if err := json.Unmarshal(value, &vs); err != nil {
		return nil, err
	}
	return decodeToValueTs(&vs, s.ValJSONSerde)
}

func (s ValueTimestampJSONSerdeG) Encode(value *ValueTimestamp) ([]byte, error) {
	vs, err := valTsToValueTsSer(value, s.ValJSONSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return json.Marshal(vs)
}

func (s ValueTimestampJSONSerdeG) Decode(value []byte) (*ValueTimestamp, error) {
	if value == nil {
		return nil, nil
	}
	vs := ValueTimestampSerialized{}
	if err := json.Unmarshal(value, &vs); err != nil {
		return nil, err
	}
	return valTsSerToValueTs(&vs, s.ValJSONSerde)
}

type ValueTimestampMsgpSerde struct {
	ValMsgpSerde Serde
}

type ValueTimestampMsgpSerdeG struct {
	ValMsgpSerde Serde
}

func (s ValueTimestampMsgpSerde) Encode(value interface{}) ([]byte, error) {
	vs, err := convertToValueTsSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return vs.MarshalMsg(nil)
}

func (s ValueTimestampMsgpSerde) Decode(value []byte) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	vs := ValueTimestampSerialized{}
	_, err := vs.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return decodeToValueTs(&vs, s.ValMsgpSerde)
}

func (s ValueTimestampMsgpSerdeG) Encode(value *ValueTimestamp) ([]byte, error) {
	vs, err := valTsToValueTsSer(value, s.ValMsgpSerde)
	if err != nil {
		return nil, err
	}
	if vs == nil {
		return nil, nil
	}
	return vs.MarshalMsg(nil)
}

func (s ValueTimestampMsgpSerdeG) Decode(value []byte) (*ValueTimestamp, error) {
	if value == nil {
		return nil, nil
	}
	vs := ValueTimestampSerialized{}
	_, err := vs.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	return valTsSerToValueTs(&vs, s.ValMsgpSerde)
}

func GetValueTsSerde(serdeFormat SerdeFormat, valSerde Serde) (Serde, error) {
	var vtSerde Serde
	if serdeFormat == JSON {
		vtSerde = ValueTimestampJSONSerde{
			ValJSONSerde: valSerde,
		}
	} else if serdeFormat == MSGP {
		vtSerde = ValueTimestampMsgpSerde{
			ValMsgpSerde: valSerde,
		}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return vtSerde, nil
}

func GetValueTsSerdeG(serdeFormat SerdeFormat, valSerde Serde) (SerdeG[*ValueTimestamp], error) {
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
