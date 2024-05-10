package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeNameCityStateId(t *testing.T) {
	v := NameCityStateId{}
	jsonSerdeG := NameCityStateIdJSONSerdeG{}
	jsonSerde := NameCityStateIdJSONSerde{}
	commtypes.GenTestEncodeDecode[NameCityStateId](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := NameCityStateIdMsgpSerdeG{}
	msgSerde := NameCityStateIdMsgpSerde{}
	commtypes.GenTestEncodeDecode[NameCityStateId](v, t, msgSerdeG, msgSerde)
}
