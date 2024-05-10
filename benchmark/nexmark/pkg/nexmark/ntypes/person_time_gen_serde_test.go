package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdePersonTime(t *testing.T) {
	v := PersonTime{}
	jsonSerdeG := PersonTimeJSONSerdeG{}
	jsonSerde := PersonTimeJSONSerde{}
	commtypes.GenTestEncodeDecode[PersonTime](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := PersonTimeMsgpSerdeG{}
	msgSerde := PersonTimeMsgpSerde{}
	commtypes.GenTestEncodeDecode[PersonTime](v, t, msgSerdeG, msgSerde)
}
