package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type TimeWindowJSONSerde struct {
	DefaultJSONSerde
}

var _ = Serde(TimeWindowJSONSerde{})

func (s TimeWindowJSONSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*TimeWindow)
	if !ok {
		vTmp := value.(TimeWindow)
		v = &vTmp
	}
	r, err := json.Marshal(v)
	return r, nil, err
}

func (s TimeWindowJSONSerde) Decode(value []byte) (interface{}, error) {
	v := TimeWindow{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

type TimeWindowMsgpSerde struct {
	DefaultMsgpSerde
}

var _ = Serde(TimeWindowMsgpSerde{})

func (s TimeWindowMsgpSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*TimeWindow)
	if !ok {
		vTmp := value.(TimeWindow)
		v = &vTmp
	}
	b := PopBuffer()
	buf := *b
	r, err := v.MarshalMsg(buf[:0])
	return r, b, err
}

func (s TimeWindowMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := TimeWindow{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return v, nil
}

func GetTimeWindowSerde(serdeFormat SerdeFormat) (Serde, error) {
	switch serdeFormat {
	case JSON:
		return TimeWindowJSONSerde{}, nil
	case MSGP:
		return TimeWindowMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}