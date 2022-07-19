//go:generate msgp
//msgp:ignore PayloadArrJSONSerde PayloadArrMsgpSerde
package commtypes

import "encoding/json"

type PayloadArr struct {
	Payloads [][]byte `json:"parr,omitempty" msg:"parr,omitempty"`
}

type PayloadArrJSONSerde struct{}

var _ = Serde[PayloadArr](PayloadArrJSONSerde{})

func (s PayloadArrJSONSerde) Encode(value PayloadArr) ([]byte, error) {
	v := &value
	return json.Marshal(v)
}

func (s PayloadArrJSONSerde) Decode(value []byte) (PayloadArr, error) {
	v := PayloadArr{}
	if err := json.Unmarshal(value, &v); err != nil {
		return PayloadArr{}, err
	}
	return v, nil
}

type PayloadArrMsgpSerde struct{}

var _ = Serde[PayloadArr](PayloadArrMsgpSerde{})

func (s PayloadArrMsgpSerde) Encode(value PayloadArr) ([]byte, error) {
	val := &value
	return val.MarshalMsg(nil)
}
func (s PayloadArrMsgpSerde) Decode(value []byte) (PayloadArr, error) {
	val := PayloadArr{}
	if _, err := val.UnmarshalMsg(value); err != nil {
		return PayloadArr{}, err
	}
	return val, nil
}
