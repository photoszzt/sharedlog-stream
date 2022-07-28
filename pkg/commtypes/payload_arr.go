//go:generate msgp
//msgp:ignore PayloadArrJSONSerde PayloadArrMsgpSerde
package commtypes

import "encoding/json"

type PayloadArr struct {
	Payloads [][]byte `json:"parr,omitempty" msg:"parr,omitempty"`
}

type PayloadArrJSONSerde struct{}

var _ = Serde(PayloadArrJSONSerde{})

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

type PayloadArrMsgpSerde struct{}

var _ = Serde(PayloadArrMsgpSerde{})

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
