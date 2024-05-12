package ntypes

import (
	"testing"

	"github.com/brianvoe/gofakeit/v7"
	"sharedlog-stream/pkg/commtypes"
)

func TestSerdePersonTime(t *testing.T) {
	faker := gofakeit.New(3)
	v := PersonTime{}
	jsonSerdeG := PersonTimeJSONSerdeG{}
	jsonSerde := PersonTimeJSONSerde{}
	msgSerdeG := PersonTimeMsgpSerdeG{}
	msgSerde := PersonTimeMsgpSerde{}
	commtypes.GenTestEncodeDecode[PersonTime](v, t, jsonSerdeG, jsonSerde)
	commtypes.GenTestEncodeDecode[PersonTime](v, t, msgSerdeG, msgSerde)
	err := faker.Struct(&v)
	if err != nil {
		t.Fatal(err)
	}
	commtypes.GenTestEncodeDecode[PersonTime](v, t, jsonSerdeG, jsonSerde)
	commtypes.GenTestEncodeDecode[PersonTime](v, t, msgSerdeG, msgSerde)
}
