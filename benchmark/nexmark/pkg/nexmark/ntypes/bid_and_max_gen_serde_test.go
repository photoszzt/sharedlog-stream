package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeBidAndMax(t *testing.T) {
	v := BidAndMax{}
	jsonSerdeG := BidAndMaxJSONSerdeG{}
	jsonSerde := BidAndMaxJSONSerde{}
	commtypes.GenTestEncodeDecode[BidAndMax](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := BidAndMaxMsgpSerdeG{}
	msgSerde := BidAndMaxMsgpSerde{}
	commtypes.GenTestEncodeDecode[BidAndMax](v, t, msgSerdeG, msgSerde)
}
