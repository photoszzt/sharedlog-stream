package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type PayloadArrJSONSerdeG struct {
	DefaultJSONSerde
}

var _ = SerdeG[*PayloadArr](PayloadArrJSONSerdeG{})

func (s PayloadArrJSONSerdeG) Encode(value *PayloadArr) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s PayloadArrJSONSerdeG) Decode(value []byte) (*PayloadArr, error) {
	v := PayloadArr{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type PayloadArrMsgpSerdeG struct {
	DefaultMsgpSerde
}

var _ = SerdeG[*PayloadArr](PayloadArrMsgpSerdeG{})

func (s PayloadArrMsgpSerdeG) Encode(value *PayloadArr) ([]byte, *[]byte, error) {
	b := PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s PayloadArrMsgpSerdeG) Decode(value []byte) (*PayloadArr, error) {
	v := PayloadArr{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetPayloadArrSerdeG(serdeFormat SerdeFormat) (SerdeG[*PayloadArr], error) {
	if serdeFormat == JSON {
		return PayloadArrJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return PayloadArrMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
