package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeAuctionIdCount(t *testing.T) {
	v := AuctionIdCount{}
	jsonSerdeG := AuctionIdCountJSONSerdeG{}
	jsonSerde := AuctionIdCountJSONSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdCount](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := AuctionIdCountMsgpSerdeG{}
	msgSerde := AuctionIdCountMsgpSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdCount](v, t, msgSerdeG, msgSerde)
}
