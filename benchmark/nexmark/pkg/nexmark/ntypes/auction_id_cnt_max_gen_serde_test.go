package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeAuctionIdCntMax(t *testing.T) {
	v := AuctionIdCntMax{}
	jsonSerdeG := AuctionIdCntMaxJSONSerdeG{}
	jsonSerde := AuctionIdCntMaxJSONSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdCntMax](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := AuctionIdCntMaxMsgpSerdeG{}
	msgSerde := AuctionIdCntMaxMsgpSerde{}
	commtypes.GenTestEncodeDecode[AuctionIdCntMax](v, t, msgSerdeG, msgSerde)
}
