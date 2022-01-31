//go:generate msgp
//msgp:ignore AuctionBidJSONSerde AuctionBidMsgpSerde
package types

import (
	"encoding/json"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type AuctionBid struct {
	BidDateTime int64  `json:"bidDateTime" msg:"bidDateTime"`
	AucDateTime int64  `json:"aucDateTime" msg:"aucDateTime"`
	AucExpires  int64  `json:"aucExpires" msg:"aucExpires"`
	BidPrice    uint64 `json:"bidPrice" msg:"bidPrice"`
	AucCategory uint64 `json:"aucCategory" msg:"aucCategory"`
}

type AuctionBidJSONSerde struct{}

var _ = commtypes.Encoder(&AuctionBidJSONSerde{})

func (s AuctionBidJSONSerde) Encode(value interface{}) ([]byte, error) {
	ab := value.(*AuctionBid)
	return json.Marshal(ab)
}

func (s AuctionBidJSONSerde) Decode(value []byte) (interface{}, error) {
	ab := AuctionBid{}
	if err := json.Unmarshal(value, &ab); err != nil {
		return nil, err
	}
	return ab, nil
}

type AuctionBidMsgpSerde struct{}

func (s AuctionBidMsgpSerde) Encode(value interface{}) ([]byte, error) {
	ab := value.(*AuctionBid)
	return ab.MarshalMsg(nil)
}

func (s AuctionBidMsgpSerde) Decode(value []byte) (interface{}, error) {
	ab := AuctionBid{}
	if _, err := ab.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return ab, nil
}
