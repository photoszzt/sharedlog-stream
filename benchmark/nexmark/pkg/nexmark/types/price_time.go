//go:generate msgp
//msgp:ignore PriceTimeJSONSerde PriceTimeMsgpSerde
package types

import (
	"encoding/json"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type PriceTime struct {
	Price    uint64 `json:"price" msg:"price"`
	DateTime int64  `json:"dateTime" msg:"dateTime"` // unix timestamp in ms

	BaseInjTime `msg:",flatten"`
}

type PriceTimeJSONSerde struct{}

var _ = commtypes.Encoder(PriceTimeJSONSerde{})

func (s PriceTimeJSONSerde) Encode(value interface{}) ([]byte, error) {
	pt := value.(*PriceTime)
	return json.Marshal(pt)
}

func (s PriceTimeJSONSerde) Decode(value []byte) (interface{}, error) {
	pt := PriceTime{}
	if err := json.Unmarshal(value, &pt); err != nil {
		return nil, err
	}
	return pt, nil
}

type PriceTimeMsgpSerde struct{}

func (s PriceTimeMsgpSerde) Encode(value interface{}) ([]byte, error) {
	pt := value.(*PriceTime)
	return pt.MarshalMsg(nil)
}

func (s PriceTimeMsgpSerde) Decode(value []byte) (interface{}, error) {
	pt := PriceTime{}
	if _, err := pt.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return pt, nil
}
