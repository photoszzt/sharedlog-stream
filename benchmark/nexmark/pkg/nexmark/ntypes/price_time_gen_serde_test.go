package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdePriceTime(t *testing.T) {
	v := PriceTime{}
	jsonSerdeG := PriceTimeJSONSerdeG{}
	jsonSerde := PriceTimeJSONSerde{}
	commtypes.GenTestEncodeDecode[PriceTime](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := PriceTimeMsgpSerdeG{}
	msgSerde := PriceTimeMsgpSerde{}
	commtypes.GenTestEncodeDecode[PriceTime](v, t, msgSerdeG, msgSerde)
}
