package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeBidPrice(t *testing.T) {
	v := BidPrice{}
	jsonSerdeG := BidPriceJSONSerdeG{}
	jsonSerde := BidPriceJSONSerde{}
	commtypes.GenTestEncodeDecode[BidPrice](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := BidPriceMsgpSerdeG{}
	msgSerde := BidPriceMsgpSerde{}
	commtypes.GenTestEncodeDecode[BidPrice](v, t, msgSerdeG, msgSerde)
}
