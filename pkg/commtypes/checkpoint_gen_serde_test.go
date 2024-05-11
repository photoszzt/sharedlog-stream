package commtypes

import (
	"testing"
)

func TestSerdeCheckpoint(t *testing.T) {
	v := Checkpoint{}
	jsonSerdeG := CheckpointJSONSerdeG{}
	jsonSerde := CheckpointJSONSerde{}
	GenTestEncodeDecode[Checkpoint](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := CheckpointMsgpSerdeG{}
	msgSerde := CheckpointMsgpSerde{}
	GenTestEncodeDecode[Checkpoint](v, t, msgSerdeG, msgSerde)
}
