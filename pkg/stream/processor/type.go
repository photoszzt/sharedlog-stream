//go:generate msgp
//msgp:ignore ValueTimestamp Change ValueTimestampJSONSerde ValueTimestampMsgpSerde
package processor

import "encoding/json"

type KeyT interface{}

type ValueT interface{}

type ValueTimestamp struct {
	Timestamp uint64
	Value     interface{}
}

type Change struct {
	OldValue interface{}
	NewValue interface{}
}

type ValueTimestampSerialized struct {
	Timestamp       uint64 `json:"ts" msg:"ts"`
	ValueSerialized []byte `json:"vs" msg:"vs"`
}

type ValueTimestampJSONSerde struct {
	ValJSONSerde Serde
}

func (s ValueTimestampJSONSerde) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(*ValueTimestamp)
	if !ok {
		vtmp := value.(ValueTimestamp)
		v = &vtmp
	}
	enc, err := s.ValJSONSerde.Encode(v.Value)
	if err != nil {
		return nil, err
	}
	vs := ValueTimestampSerialized{
		Timestamp:       v.Timestamp,
		ValueSerialized: enc,
	}
	return json.Marshal(&vs)
}

func (s ValueTimestampJSONSerde) Decode(value []byte) (interface{}, error) {
	vs := ValueTimestampSerialized{}
	if err := json.Unmarshal(value, &vs); err != nil {
		return nil, err
	}
	v, err := s.ValJSONSerde.Decode(vs.ValueSerialized)
	if err != nil {
		return nil, err
	}
	return ValueTimestamp{
		Timestamp: vs.Timestamp,
		Value:     v,
	}, nil
}

type ValueTimestampMsgpSerde struct {
	ValMsgpSerde Serde
}

func (s ValueTimestampMsgpSerde) Encode(value interface{}) ([]byte, error) {
	v, ok := value.(*ValueTimestamp)
	if !ok {
		vtmp := value.(ValueTimestamp)
		v = &vtmp
	}
	enc, err := s.ValMsgpSerde.Encode(v.Value)
	if err != nil {
		return nil, err
	}
	vs := ValueTimestampSerialized{
		Timestamp:       v.Timestamp,
		ValueSerialized: enc,
	}
	return vs.MarshalMsg(nil)
}

func (s ValueTimestampMsgpSerde) Decode(value []byte) (interface{}, error) {
	vs := ValueTimestampSerialized{}
	_, err := vs.UnmarshalMsg(value)
	if err != nil {
		return nil, err
	}
	v, err := s.ValMsgpSerde.Decode(vs.ValueSerialized)
	if err != nil {
		return nil, err
	}
	return ValueTimestamp{
		Timestamp: vs.Timestamp,
		Value:     v,
	}, nil
}
