//go:generate msgp
//msgp:ignore TxnMetadataJSONSerde TxnMetadataMsgpSerde
package txn_data

import (
	"encoding/json"
	"fmt"
	"sharedlog-stream/pkg/commtypes"
)

type TransactionState uint8

const (
	EMPTY = TransactionState(iota) // transaction has not existed yet
	BEGIN                          // transaction has started and ongoing
	PREPARE_COMMIT
	PREPARE_ABORT
	COMPLETE_COMMIT
	COMPLETE_ABORT
	FENCE
)

func (ts TransactionState) String() string {
	return []string{"EMPTY", "BEGIN", "PREPARE_COMMIT", "PREPARE_ABORT", "COMPLETE_COMMIT", "COMPLETE_ABORT", "FENCE"}[ts]
}

func (ts TransactionState) IsValidPreviousState(prevState TransactionState) bool {
	switch ts {
	case EMPTY:
		return prevState == EMPTY || prevState == COMPLETE_ABORT || prevState == COMPLETE_COMMIT || prevState == FENCE
	case BEGIN:
		return prevState == BEGIN || prevState == EMPTY || prevState == COMPLETE_ABORT || prevState == COMPLETE_COMMIT || prevState == FENCE
	case PREPARE_COMMIT:
		return prevState == BEGIN
	case PREPARE_ABORT:
		return prevState == BEGIN || prevState == FENCE
	case COMPLETE_ABORT:
		return prevState == PREPARE_ABORT
	case COMPLETE_COMMIT:
		return prevState == PREPARE_COMMIT
	case FENCE:
		return prevState == EMPTY
	default:
		panic(fmt.Sprintf("transaction state is not recognized: %d", ts))
	}
}

// log entries in transaction log
type TxnMetadata struct {
	TopicPartitions []TopicPartition `json:"tp,omitempty" msg:"tp,omitempty"`
	State           TransactionState `json:"st,omitempty" msg:"st,omitempty"`
}

type TxnMetadataJSONSerde struct{}
type TxnMetadataJSONSerdeG struct{}

var _ = commtypes.Serde(TxnMetadataJSONSerde{})
var _ = commtypes.SerdeG[TxnMetadata](TxnMetadataJSONSerdeG{})

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

type TxnMetadataMsgpSerde struct{}
type TxnMetadataMsgpSerdeG struct{}

var _ = commtypes.Serde(TxnMetadataMsgpSerde{})
var _ = commtypes.SerdeG[TxnMetadata](TxnMetadataMsgpSerdeG{})

func (s TxnMetadataMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tm := value.(*TxnMetadata)
	return tm.MarshalMsg(nil)
}

func (s TxnMetadataMsgpSerdeG) Encode(value TxnMetadata) ([]byte, error) {
	return value.MarshalMsg(nil)
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
