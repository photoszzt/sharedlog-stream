//go:generate msgp
//msgp:ignore BidPriceJSONSerde BidPriceMsgpSerde
package types

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type BidPrice struct {
	Price uint64 `json:"price" msg:"price"`
}

type BidPriceJSONSerde struct{}
type BidPriceMsgpSerde struct{}

var _ = commtypes.Serde(BidPriceJSONSerde{})

func (s BidPriceJSONSerde) Encode(value interface{}) ([]byte, error) {
	bp := value.(*BidPrice)
	return json.Marshal(bp)
}

func (s BidPriceJSONSerde) Decode(value []byte) (interface{}, error) {
	bp := BidPrice{}
	if err := json.Unmarshal(value, &bp); err != nil {
		return nil, err
	}
	return bp, nil
}

var _ = commtypes.Serde(BidPriceMsgpSerde{})

func (s BidPriceMsgpSerde) Encode(value interface{}) ([]byte, error) {
	bp := value.(*BidPrice)
	return bp.MarshalMsg(nil)
}

func (s BidPriceMsgpSerde) Decode(value []byte) (interface{}, error) {
	bp := BidPrice{}
	if _, err := bp.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return bp, nil
}

func GetBidPriceSerde(serde commtypes.SerdeFormat) (commtypes.Serde, error) {
	if serde == commtypes.JSON {
		return BidPriceJSONSerde{}, nil
	} else if serde == commtypes.MSGP {
		return BidPriceMsgpSerde{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}