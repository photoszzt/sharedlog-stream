package commtypes

import (
	"testing"

	"github.com/brianvoe/gofakeit/v7"
)

func TestSerdeTimeWindow(t *testing.T) {
	faker := gofakeit.New(3)
	v := TimeWindow{}
	jsonSerdeG := TimeWindowJSONSerdeG{}
	jsonSerde := TimeWindowJSONSerde{}
	msgSerdeG := TimeWindowMsgpSerdeG{}
	msgSerde := TimeWindowMsgpSerde{}
	GenTestEncodeDecode[TimeWindow](v, t, jsonSerdeG, jsonSerde)
	GenTestEncodeDecode[TimeWindow](v, t, msgSerdeG, msgSerde)
	err := faker.Struct(&v)
	if err != nil {
		t.Fatal(err)
	}
	GenTestEncodeDecode[TimeWindow](v, t, jsonSerdeG, jsonSerde)
	GenTestEncodeDecode[TimeWindow](v, t, msgSerdeG, msgSerde)
}
