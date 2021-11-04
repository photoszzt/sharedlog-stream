//go:generate msgp
//msgp:ignore ValueTimestampJSONSerde ValueTimestampMsgpSerde
package commtypes

import "encoding/json"

type ValueTimestamp struct {
	Value     interface{}
	Timestamp uint64
}

type ValueTimestampSerialized struct {
	ValueSerialized []byte `json:"vs" msg:"vs"`
	Timestamp       uint64 `json:"ts" msg:"ts"`
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
