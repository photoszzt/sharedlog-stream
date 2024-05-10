package txn_data

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeOffsetRecord(t *testing.T) {
	v := OffsetRecord{}
	jsonSerdeG := OffsetRecordJSONSerdeG{}
	jsonSerde := OffsetRecordJSONSerde{}
	commtypes.GenTestEncodeDecode[OffsetRecord](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := OffsetRecordMsgpSerdeG{}
	msgSerde := OffsetRecordMsgpSerde{}
	commtypes.GenTestEncodeDecode[OffsetRecord](v, t, msgSerdeG, msgSerde)
}
