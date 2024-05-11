package commtypes

import (
	"testing"
)

func TestSerdePayloadArr(t *testing.T) {
	v := PayloadArr{}
	jsonSerdeG := PayloadArrJSONSerdeG{}
	jsonSerde := PayloadArrJSONSerde{}
	GenTestEncodeDecode[PayloadArr](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := PayloadArrMsgpSerdeG{}
	msgSerde := PayloadArrMsgpSerde{}
	GenTestEncodeDecode[PayloadArr](v, t, msgSerdeG, msgSerde)
}
