//go:generate msgp
//msgp:ignore PayloadTsJSONSerde PayloadTsMsgpSerde
package datatype

import "sharedlog-stream/pkg/commtypes"

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

var _ commtypes.EventTimeExtractor = PayloadTs{}

func (pt PayloadTs) ExtractEventTime() (int64, error) {
	return int64(pt.Ts / 1000.0), nil
}
