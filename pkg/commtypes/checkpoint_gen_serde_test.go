package commtypes

import (
	"testing"

	"github.com/brianvoe/gofakeit/v7"
)

func TestSerdeCheckpoint(t *testing.T) {
	faker := gofakeit.New(3)
	v := Checkpoint{}
	jsonSerdeG := CheckpointJSONSerdeG{}
	jsonSerde := CheckpointJSONSerde{}
	msgSerdeG := CheckpointMsgpSerdeG{}
	msgSerde := CheckpointMsgpSerde{}
	GenTestEncodeDecode[Checkpoint](v, t, jsonSerdeG, jsonSerde)
	GenTestEncodeDecode[Checkpoint](v, t, msgSerdeG, msgSerde)
	err := faker.Struct(&v)
	if err != nil {
		t.Fatal(err)
	}
	GenTestEncodeDecode[Checkpoint](v, t, jsonSerdeG, jsonSerde)
	GenTestEncodeDecode[Checkpoint](v, t, msgSerdeG, msgSerde)
}
