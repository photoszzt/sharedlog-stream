package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PersonTimeJSONSerde struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.Serde(PersonTimeJSONSerde{})

func (s PersonTimeJSONSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*PersonTime)
	if !ok {
		vTmp := value.(PersonTime)
		v = &vTmp
	}
	r, err := json.Marshal(v)
	return r, nil, err
}

func (s PersonTimeJSONSerde) Decode(value []byte) (interface{}, error) {
	v := PersonTime{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

type PersonTimeMsgpSerde struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.Serde(PersonTimeMsgpSerde{})

func (s PersonTimeMsgpSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*PersonTime)
	if !ok {
		vTmp := value.(PersonTime)
		v = &vTmp
	}
	b := commtypes.PopBuffer()
	buf := *b
	r, err := v.MarshalMsg(buf[:0])
	return r, b, err
}

func (s PersonTimeMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := PersonTime{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return v, nil
}

func GetPersonTimeSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return PersonTimeJSONSerde{}, nil
	case commtypes.MSGP:
		return PersonTimeMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
