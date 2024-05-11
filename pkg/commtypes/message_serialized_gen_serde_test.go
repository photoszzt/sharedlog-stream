package commtypes

import (
	"testing"
)

func TestSerdeMessageSerialized(t *testing.T) {
	v := MessageSerialized{}
	jsonSerdeG := MessageSerializedJSONSerdeG{}
	jsonSerde := MessageSerializedJSONSerde{}
	GenTestEncodeDecode[MessageSerialized](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := MessageSerializedMsgpSerdeG{}
	msgSerde := MessageSerializedMsgpSerde{}
	GenTestEncodeDecode[MessageSerialized](v, t, msgSerdeG, msgSerde)
}
