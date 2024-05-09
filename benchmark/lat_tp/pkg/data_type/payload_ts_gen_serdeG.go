package datatype

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type PayloadTsJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[PayloadTs](PayloadTsJSONSerdeG{})

func (s PayloadTsJSONSerdeG) Encode(value PayloadTs) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s PayloadTsJSONSerdeG) Decode(value []byte) (PayloadTs, error) {
	v := PayloadTs{}
	if err := json.Unmarshal(value, &v); err != nil {
		return PayloadTs{}, err
	}
	return v, nil
}

type PayloadTsMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[PayloadTs](PayloadTsMsgpSerdeG{})

func (s PayloadTsMsgpSerdeG) Encode(value PayloadTs) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s PayloadTsMsgpSerdeG) Decode(value []byte) (PayloadTs, error) {
	v := PayloadTs{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return PayloadTs{}, err
	}
	return v, nil
}

func GetPayloadTsSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[PayloadTs], error) {
	if serdeFormat == commtypes.JSON {
		return PayloadTsJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return PayloadTsMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
