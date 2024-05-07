package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type PayloadArrJSONSerdeG struct {
	DefaultJSONSerde
}

var _ = SerdeG[PayloadArr](PayloadArrJSONSerdeG{})

func (s PayloadArrJSONSerdeG) Encode(value PayloadArr) ([]byte, error) {
	return json.Marshal(&value)
}

func (s PayloadArrJSONSerdeG) Decode(value []byte) (PayloadArr, error) {
	v := PayloadArr{}
	if err := json.Unmarshal(value, &v); err != nil {
		return PayloadArr{}, err
	}
	return v, nil
}

type PayloadArrMsgpSerdeG struct {
	DefaultMsgpSerde
}

var _ = SerdeG[PayloadArr](PayloadArrMsgpSerdeG{})

func (s PayloadArrMsgpSerdeG) Encode(value PayloadArr) ([]byte, error) {
	return value.MarshalMsg(nil)
}

func (s PayloadArrMsgpSerdeG) Decode(value []byte) (PayloadArr, error) {
	val := PayloadArr{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return PayloadArr{}, err
	}
	return val, nil
}

func GetPayloadArrSerdeG(serdeFormat SerdeFormat) (SerdeG[PayloadArr], error) {
	switch serdeFormat {
	case JSON:
		return PayloadArrJSONSerdeG{}, nil
	case MSGP:
		return PayloadArrMsgpSerdeG{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
