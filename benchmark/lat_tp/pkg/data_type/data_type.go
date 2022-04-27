//go:generate msgp
package datatype

type PayloadTs struct {
	Payload []byte `json:"pl" msg:"pl"`
	Ts      int64  `json:"ts" msg:"ts"`
}

type PayloadTsMsgpSerde struct{}

func (s PayloadTsMsgpSerde) Encode(value interface{}) ([]byte, error) {
	pt := value.(*PayloadTs)
	return pt.MarshalMsg(nil)
}

func (s PayloadTsMsgpSerde) Decode(value []byte) (interface{}, error) {
	pt := PayloadTs{}
	if _, err := pt.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return pt, nil
}
