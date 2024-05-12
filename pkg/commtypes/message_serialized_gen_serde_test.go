package commtypes

import (
	"testing"

	"github.com/brianvoe/gofakeit/v7"
)

func TestSerdeMessageSerialized(t *testing.T) {
	faker := gofakeit.New(3)
	v := MessageSerialized{}
	jsonSerdeG := MessageSerializedJSONSerdeG{}
	jsonSerde := MessageSerializedJSONSerde{}
	msgSerdeG := MessageSerializedMsgpSerdeG{}
	msgSerde := MessageSerializedMsgpSerde{}
	GenTestEncodeDecode[MessageSerialized](v, t, jsonSerdeG, jsonSerde)
	GenTestEncodeDecode[MessageSerialized](v, t, msgSerdeG, msgSerde)
	err := faker.Struct(&v)
	if err != nil {
		t.Fatal(err)
	}
	GenTestEncodeDecode[MessageSerialized](v, t, jsonSerdeG, jsonSerde)
	GenTestEncodeDecode[MessageSerialized](v, t, msgSerdeG, msgSerde)
}
