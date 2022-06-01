//go:generate msgp
//msgp:ignore ValueTimestampJSONSerde ValueTimestampMsgpSerde ValueTimestamp
package commtypes

import (
	"encoding/json"
	"fmt"
)

type ValueTimestamp struct {
	Value     interface{}
	Timestamp int64
	BaseInjTime
}

type ValueTimestampSerialized struct {
	ValueSerialized []byte `json:"vs,omitempty" msg:"vs,omitempty"`
	Timestamp       int64  `json:"ts,omitempty" msg:"ts,omitempty"`
	InjectToStream  int64  `msg:"injT" json:"injT"`
}

func (s *ValueTimestamp) ExtractEventTime() (int64, error) {
	return s.Timestamp, nil
}

var _ = EventTimeExtractor(&ValueTimestamp{})

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
		InjectToStream:  v.BaseInjTime.InjT,
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
		BaseInjTime: BaseInjTime{
			InjT: vs.InjectToStream,
		},
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
		InjectToStream:  v.BaseInjTime.InjT,
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
		BaseInjTime: BaseInjTime{
			InjT: vs.InjectToStream,
		},
	}, nil
}

func GetValueTsSerde(serdeFormat SerdeFormat, valJSONSerde, valMsgpSerde Serde) (Serde, error) {
	var vtSerde Serde
	if serdeFormat == JSON {
		vtSerde = ValueTimestampJSONSerde{
			ValJSONSerde: valJSONSerde,
		}
	} else if serdeFormat == MSGP {
		vtSerde = ValueTimestampMsgpSerde{
			ValMsgpSerde: valMsgpSerde,
		}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return vtSerde, nil
}
