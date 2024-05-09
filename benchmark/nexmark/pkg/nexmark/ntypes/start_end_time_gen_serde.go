package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type StartEndTimeJSONSerde struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.Serde(StartEndTimeJSONSerde{})

func (s StartEndTimeJSONSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*StartEndTime)
	if !ok {
		vTmp := value.(StartEndTime)
		v = &vTmp
	}
	r, err := json.Marshal(v)
	return r, nil, err
}

func (s StartEndTimeJSONSerde) Decode(value []byte) (interface{}, error) {
	v := StartEndTime{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

type StartEndTimeMsgpSerde struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.Serde(StartEndTimeMsgpSerde{})

func (s StartEndTimeMsgpSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*StartEndTime)
	if !ok {
		vTmp := value.(StartEndTime)
		v = &vTmp
	}
	b := commtypes.PopBuffer()
	buf := *b
	r, err := v.MarshalMsg(buf[:0])
	return r, b, err
}

func (s StartEndTimeMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := StartEndTime{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return v, nil
}

func GetStartEndTimeSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return StartEndTimeJSONSerde{}, nil
	case commtypes.MSGP:
		return StartEndTimeMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
