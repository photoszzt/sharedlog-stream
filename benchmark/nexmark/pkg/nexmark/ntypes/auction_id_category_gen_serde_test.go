package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeAuctionIdCategory(t *testing.T) {
	v := AuctionIdCategory{}
	jsonSerdeG := AuctionIdCategoryJSONSerdeG{}
	jsonSerde := AuctionIdCategoryJSONSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdCategory](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := AuctionIdCategoryMsgpSerdeG{}
	msgSerde := AuctionIdCategoryMsgpSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdCategory](v, t, msgSerdeG, msgSerde)
}
