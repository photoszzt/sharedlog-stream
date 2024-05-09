package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type TimeWindowJSONSerdeG struct {
	DefaultJSONSerde
}

var _ = SerdeG[TimeWindow](TimeWindowJSONSerdeG{})

func (s TimeWindowJSONSerdeG) Encode(value TimeWindow) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s TimeWindowJSONSerdeG) Decode(value []byte) (TimeWindow, error) {
	v := TimeWindow{}
	if err := json.Unmarshal(value, &v); err != nil {
		return TimeWindow{}, err
	}
	return v, nil
}

type TimeWindowMsgpSerdeG struct {
	DefaultMsgpSerde
}

var _ = SerdeG[TimeWindow](TimeWindowMsgpSerdeG{})

func (s TimeWindowMsgpSerdeG) Encode(value TimeWindow) ([]byte, *[]byte, error) {
	b := PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s TimeWindowMsgpSerdeG) Decode(value []byte) (TimeWindow, error) {
	v := TimeWindow{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return TimeWindow{}, err
	}
	return v, nil
}

func GetTimeWindowSerdeG(serdeFormat SerdeFormat) (SerdeG[TimeWindow], error) {
	if serdeFormat == JSON {
		return TimeWindowJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return TimeWindowMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
