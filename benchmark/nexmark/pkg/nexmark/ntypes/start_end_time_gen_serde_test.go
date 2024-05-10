package ntypes

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeStartEndTime(t *testing.T) {
	v := StartEndTime{}
	jsonSerdeG := StartEndTimeJSONSerdeG{}
	jsonSerde := StartEndTimeJSONSerde{}
	commtypes.GenTestEncodeDecode[StartEndTime](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := StartEndTimeMsgpSerdeG{}
	msgSerde := StartEndTimeMsgpSerde{}
	commtypes.GenTestEncodeDecode[StartEndTime](v, t, msgSerdeG, msgSerde)
}
