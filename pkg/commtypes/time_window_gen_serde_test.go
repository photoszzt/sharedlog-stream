package commtypes

import (
	"testing"
)

func TestSerdeTimeWindow(t *testing.T) {
	v := TimeWindow{}
	jsonSerdeG := TimeWindowJSONSerdeG{}
	jsonSerde := TimeWindowJSONSerde{}
	GenTestEncodeDecode[TimeWindow](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := TimeWindowMsgpSerdeG{}
	msgSerde := TimeWindowMsgpSerde{}
	GenTestEncodeDecode[TimeWindow](v, t, msgSerdeG, msgSerde)
}
