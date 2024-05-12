package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type StartEndTimeJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[StartEndTime](StartEndTimeJSONSerdeG{})

func (s StartEndTimeJSONSerdeG) Encode(value StartEndTime) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s StartEndTimeJSONSerdeG) Decode(value []byte) (StartEndTime, error) {
	v := StartEndTime{}
	if err := json.Unmarshal(value, &v); err != nil {
		return StartEndTime{}, err
	}
	return v, nil
}

type StartEndTimeMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[StartEndTime](StartEndTimeMsgpSerdeG{})

func (s StartEndTimeMsgpSerdeG) Encode(value StartEndTime) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s StartEndTimeMsgpSerdeG) Decode(value []byte) (StartEndTime, error) {
	v := StartEndTime{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return StartEndTime{}, err
	}
	return v, nil
}

func GetStartEndTimeSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[StartEndTime], error) {
	if serdeFormat == commtypes.JSON {
		return StartEndTimeJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return StartEndTimeMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
