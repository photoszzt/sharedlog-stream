package txn_data

import (
	"sharedlog-stream/pkg/commtypes"
	"testing"
)

func TestSerdeTxnMetadata(t *testing.T) {
	v := TxnMetadata{}
	jsonSerdeG := TxnMetadataJSONSerdeG{}
	jsonSerde := TxnMetadataJSONSerde{}
	commtypes.GenTestEncodeDecode[TxnMetadata](v, t, jsonSerdeG, jsonSerde)
	msgSerdeG := TxnMetadataMsgpSerdeG{}
	msgSerde := TxnMetadataMsgpSerde{}
	commtypes.GenTestEncodeDecode[TxnMetadata](v, t, msgSerdeG, msgSerde)
}
