package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/commtypes"
)

type (
	TxnMetadataJSONSerde struct {
		commtypes.DefaultJSONSerde
	}
	TxnMetadataJSONSerdeG struct {
		commtypes.DefaultJSONSerde
	}
)

var (
	_ = commtypes.Serde(TxnMetadataJSONSerde{})
	_ = commtypes.SerdeG[TxnMetadata](TxnMetadataJSONSerdeG{})
)

func (s TxnMetadataJSONSerde) Encode(value interface{}) ([]byte, error) {
	tm := value.(*TxnMetadata)
	return json.Marshal(tm)
}

func (s TxnMetadataJSONSerdeG) Encode(value TxnMetadata) ([]byte, error) {
	return json.Marshal(&value)
}

func (s TxnMetadataJSONSerde) Decode(value []byte) (interface{}, error) {
	tm := TxnMetadata{}
	if err := json.Unmarshal(value, &tm); err != nil {
		return nil, err
	}
	return tm, nil
}

func (s TxnMetadataJSONSerdeG) Decode(value []byte) (TxnMetadata, error) {
	tm := TxnMetadata{}
	if err := json.Unmarshal(value, &tm); err != nil {
		return TxnMetadata{}, err
	}
	return tm, nil
}

type (
	TxnMetadataMsgpSerde struct {
		commtypes.DefaultMsgpSerde
	}
	TxnMetadataMsgpSerdeG struct {
		commtypes.DefaultMsgpSerde
	}
)

var (
	_ = commtypes.Serde(TxnMetadataMsgpSerde{})
	_ = commtypes.SerdeG[TxnMetadata](TxnMetadataMsgpSerdeG{})
)

func (s TxnMetadataMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tm := value.(*TxnMetadata)
	b := commtypes.PopBuffer()
	buf := *b
	return tm.MarshalMsg(buf[:0])
}

func (s TxnMetadataMsgpSerdeG) Encode(value TxnMetadata) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s TxnMetadataMsgpSerde) Decode(value []byte) (interface{}, error) {
	tm := TxnMetadata{}
	if _, err := tm.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return tm, nil
}

func (s TxnMetadataMsgpSerdeG) Decode(value []byte) (TxnMetadata, error) {
	tm := TxnMetadata{}
	if _, err := tm.UnmarshalMsg(value); err != nil {
		return TxnMetadata{}, err
	}
	return tm, nil
}
