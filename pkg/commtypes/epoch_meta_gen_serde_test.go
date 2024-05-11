package commtypes

import (
	"testing"
)

func TestSerdeEpochMarker(t *testing.T) {
	v := EpochMarker{}
	jsonSerdeG := EpochMarkerJSONSerdeG{}
	jsonSerde := EpochMarkerJSONSerde{}
	GenTestEncodeDecode[EpochMarker](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := EpochMarkerMsgpSerdeG{}
	msgSerde := EpochMarkerMsgpSerde{}
	GenTestEncodeDecode[EpochMarker](v, t, msgSerdeG, msgSerde)
}
