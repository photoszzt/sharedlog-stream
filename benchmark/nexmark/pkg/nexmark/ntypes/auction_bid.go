//go:generate msgp
//msgp:ignore AuctionBidJSONSerde AuctionBidMsgpSerde AuctionBidMsgpSerdeG AuctionBidJSONSerdeG
package ntypes

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type AuctionBid struct {
	BidDateTime int64  `json:"bidDateTime" msg:"bidDateTime"`
	AucDateTime int64  `json:"aucDateTime" msg:"aucDateTime"`
	AucExpires  int64  `json:"aucExpires" msg:"aucExpires"`
	BidPrice    uint64 `json:"bidPrice" msg:"bidPrice"`
	AucCategory uint64 `json:"aucCategory" msg:"aucCategory"`
	AucSeller   uint64 `json:"aucSeller,omitempty" msg:"aucSeller,omitempty"`
}

var _ = fmt.Stringer(AuctionBid{})

func (ab AuctionBid) String() string {
	return fmt.Sprintf("AuctionBid: {BidTs: %d, AucTs: %d, AucExpiresTs: %d, BidPrice: %d, AucCat: %d}",
		ab.BidDateTime, ab.AucDateTime, ab.AucExpires, ab.BidPrice, ab.AucCategory)
}

type AuctionBidJSONSerde struct{}

var _ = commtypes.Serde(AuctionBidJSONSerde{})

func (s AuctionBidJSONSerde) Encode(value interface{}) ([]byte, error) {
	ab := value.(*AuctionBid)
	return json.Marshal(ab)
}

func (s AuctionBidJSONSerde) Decode(value []byte) (interface{}, error) {
	ab := AuctionBid{}
	if err := json.Unmarshal(value, &ab); err != nil {
		return nil, err
	}
	return &ab, nil
}

type AuctionBidMsgpSerde struct{}

var _ = commtypes.Serde(AuctionBidMsgpSerde{})

func (s AuctionBidMsgpSerde) Encode(value interface{}) ([]byte, error) {
	ab := value.(*AuctionBid)
	return ab.MarshalMsg(nil)
}

func (s AuctionBidMsgpSerde) Decode(value []byte) (interface{}, error) {
	ab := AuctionBid{}
	if _, err := ab.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &ab, nil
}

func GetAuctionBidSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return AuctionBidJSONSerde{}, nil
	case commtypes.MSGP:
		return AuctionBidMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}