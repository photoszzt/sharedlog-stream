//go:generate msgp
//msgp:ignore TxnMetadataJSONSerde TxnMetadataMsgpSerde
package sharedlog_stream

import (
	"encoding/json"
	"fmt"
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
		return prevState == EMPTY || prevState == COMPLETE_ABORT || prevState == COMPLETE_COMMIT
	case BEGIN:
		return prevState == BEGIN || prevState == EMPTY || prevState == COMPLETE_ABORT || prevState == COMPLETE_COMMIT
	case PREPARE_COMMIT:
		return prevState == BEGIN
	case PREPARE_ABORT:
		return prevState == BEGIN
	case COMPLETE_ABORT:
		return prevState == PREPARE_ABORT || prevState == FENCE
	case COMPLETE_COMMIT:
		return prevState == PREPARE_COMMIT
	case FENCE:
		return prevState == BEGIN
	default:
		panic(fmt.Sprintf("transaction state is not recognized: %d", ts))
	}
}

// log entries in transaction log
type TxnMetadata struct {
	TopicPartitions []TopicPartition `json:"tp,omitempty" msg:"tp,omitempty"`
	State           TransactionState `json:"st" msg:"st"`
}

type TxnMetadataJSONSerde struct{}

func (s TxnMetadataJSONSerde) Encode(value interface{}) ([]byte, error) {
	tm := value.(*TxnMetadata)
	return json.Marshal(tm)
}

func (s TxnMetadataJSONSerde) Decode(value []byte) (interface{}, error) {
	tm := TxnMetadata{}
	if err := json.Unmarshal(value, &tm); err != nil {
		return nil, err
	}
	return tm, nil
}

type TxnMetadataMsgpSerde struct{}

func (s TxnMetadataMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tm := value.(*TxnMetadata)
	return tm.UnmarshalMsg(nil)
}

func (s TxnMetadataMsgpSerde) Decode(value []byte) (interface{}, error) {
	tm := TxnMetadata{}
	if _, err := tm.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return tm, nil
}
