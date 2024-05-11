package datatype

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdePayloadTs(t *testing.T) {
	v := PayloadTs{}
	jsonSerdeG := PayloadTsJSONSerdeG{}
	jsonSerde := PayloadTsJSONSerde{}
	commtypes.GenTestEncodeDecode[PayloadTs](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := PayloadTsMsgpSerdeG{}
	msgSerde := PayloadTsMsgpSerde{}
	commtypes.GenTestEncodeDecode[PayloadTs](v, t, msgSerdeG, msgSerde)
}
