package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeSumAndCount(t *testing.T) {
	v := SumAndCount{}
	jsonSerdeG := SumAndCountJSONSerdeG{}
	jsonSerde := SumAndCountJSONSerde{}
	commtypes.GenTestEncodeDecode[SumAndCount](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := SumAndCountMsgpSerdeG{}
	msgSerde := SumAndCountMsgpSerde{}
	commtypes.GenTestEncodeDecode[SumAndCount](v, t, msgSerdeG, msgSerde)
}
