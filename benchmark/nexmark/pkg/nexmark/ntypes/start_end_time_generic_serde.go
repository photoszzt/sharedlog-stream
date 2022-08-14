package ntypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type StartEndTimeJSONSerdeG struct{}

var _ = commtypes.SerdeG[StartEndTime](StartEndTimeJSONSerdeG{})

func (e StartEndTimeJSONSerdeG) Encode(value StartEndTime) ([]byte, error) {
	return json.Marshal(&value)
}

func (d StartEndTimeJSONSerdeG) Decode(value []byte) (StartEndTime, error) {
	se := StartEndTime{}
	err := json.Unmarshal(value, &se)
	if err != nil {
		return StartEndTime{}, err
	}
	return se, nil
}

type StartEndTimeMsgpSerdeG struct{}

var _ = commtypes.SerdeG[StartEndTime](StartEndTimeMsgpSerdeG{})

func (e StartEndTimeMsgpSerdeG) Encode(value StartEndTime) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (d StartEndTimeMsgpSerdeG) Decode(value []byte) (StartEndTime, error) {
	se := StartEndTime{}
	_, err := se.UnmarshalMsg(value)
	if err != nil {
		return StartEndTime{}, err
	}
	return se, nil
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
