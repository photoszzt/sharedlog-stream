//go:generate msgp
//msgp:ignore PayloadArrJSONSerde PayloadArrMsgpSerde
package commtypes

import "encoding/json"

type PayloadArr struct {
	Payloads [][]byte `json:"parr,omitempty" msg:"parr,omitempty"`
}

type PayloadArrJSONSerde struct{}
type PayloadArrJSONSerdeG struct{}

var _ = Serde(PayloadArrJSONSerde{})
var _ = SerdeG[PayloadArr](PayloadArrJSONSerdeG{})

func (s PayloadArrJSONSerde) Encode(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}
	v := value.(*PayloadArr)
	return json.Marshal(v)
}

func (s PayloadArrJSONSerde) Decode(value []byte) (interface{}, error) {
	v := PayloadArr{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

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

type PayloadArrMsgpSerde struct{}
type PayloadArrMsgpSerdeG struct{}

var _ = Serde(PayloadArrMsgpSerde{})
var _ = SerdeG[PayloadArr](PayloadArrMsgpSerdeG{})

func (s PayloadArrMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*PayloadArr)
	return val.MarshalMsg(nil)
}
func (s PayloadArrMsgpSerde) Decode(value []byte) (interface{}, error) {
	val := PayloadArr{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return val, nil
}

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
