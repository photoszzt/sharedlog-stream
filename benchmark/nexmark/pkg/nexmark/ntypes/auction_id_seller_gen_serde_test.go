package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeAuctionIdSeller(t *testing.T) {
	v := AuctionIdSeller{}
	jsonSerdeG := AuctionIdSellerJSONSerdeG{}
	jsonSerde := AuctionIdSellerJSONSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdSeller](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := AuctionIdSellerMsgpSerdeG{}
	msgSerde := AuctionIdSellerMsgpSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdSeller](v, t, msgSerdeG, msgSerde)
}
