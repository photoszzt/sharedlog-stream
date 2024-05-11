package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdePriceTimeList(t *testing.T) {
	v := PriceTimeList{}
	jsonSerdeG := PriceTimeListJSONSerdeG{}
	jsonSerde := PriceTimeListJSONSerde{}
	commtypes.GenTestEncodeDecode[PriceTimeList](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := PriceTimeListMsgpSerdeG{}
	msgSerde := PriceTimeListMsgpSerde{}
	commtypes.GenTestEncodeDecode[PriceTimeList](v, t, msgSerdeG, msgSerde)
}
