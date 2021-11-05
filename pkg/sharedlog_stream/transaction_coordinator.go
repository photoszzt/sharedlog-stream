//go:generate msgp
//msgp:ignore TransactionManager
package sharedlog_stream

import (
	"context"
	"fmt"

	"sharedlog-stream/pkg/stream/processor/commtypes"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

const (
	TRANSACTION_LOG_TOPIC_NAME     = "__transaction_log"
	CONSUMER_OFFSET_LOG_TOPIC_NAME = "__offset_log"
	NUM_TRANSACTION_LOG_PARTITION  = 50
)

type TransactionState uint8

const (
	EMPTY = TransactionState(iota) // transaction has not existed yet
	BEGIN                          // transaction has started and ongoing
	PREPARE_COMMIT
	PREPARE_ABORT
	COMPLETE_COMMIT
	COMPLETE_ABORT
)

func (ts TransactionState) String() string {
	return []string{"EMPTY", "BEGIN", "PREPARE_COMMIT", "PREPARE_ABORT", "COMPLETE_COMMIT", "COMPLETE_ABORT"}[ts]
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
		return prevState == PREPARE_ABORT
	case COMPLETE_COMMIT:
		return prevState == PREPARE_COMMIT
	default:
		panic(fmt.Sprintf("transaction state is not recognized: %d", ts))
	}
}

type TopicPartition struct {
	Topic  string  `json:"topic" msg:"topic"`
	ParNum []uint8 `json:"parnum" msg:"parnum"`
}

// log entries in transaction log
type TxnMetadata struct {
	State TransactionState `json:"st" msg:"st"`
}

type TxnMark uint8

const (
	COMMIT TxnMark = 0
	ABORT  TxnMark = 1
)

type TxnMarker struct {
	Mark     uint8  `json:"mk" msg:"mk"`
	AppEpoch uint16 `json:"ae" msg:"ae"`
	AppId    uint64 `json:"aid" msg:"aid"`
}

type TransactionManager struct {
	payloadSerde    commtypes.Serde
	msgSerde        commtypes.MsgSerde
	TransactionLog  *SharedLogStream
	TransactionalId string
	currentStatus   TransactionState
}

func NewTransactionCoordinator(ctx context.Context, env types.Environment, transactional_id string) (*TransactionManager, error) {
	log, err := NewSharedLogStream(ctx, env,
		TRANSACTION_LOG_TOPIC_NAME+"_"+transactional_id)
	if err != nil {
		return nil, err
	}
	return &TransactionManager{
		TransactionLog:  log,
		TransactionalId: transactional_id,
	}, nil
}

func (tc *TransactionManager) InitTransaction() {
	// Steps:
	// 1. roll forward/backward the transactions that are not finished
}

func (tc *TransactionManager) RegisterTopicPartition(topic string, parNum []uint8) error {
	if tc.currentStatus != BEGIN {
		return xerrors.Errorf("should begin transaction first")
	}
	tp := TopicPartition{
		Topic:  topic,
		ParNum: parNum,
	}
	encoded, err := tc.payloadSerde.Encode(tp)
	if err != nil {
		return err
	}
	msg_encoded, err := tc.msgSerde.Encode(nil, encoded)
	if err != nil {
		return err
	}
	_, err = tc.TransactionLog.Push(msg_encoded, 0, nil)
	return err
}

func (tc *TransactionManager) BeginTransaction() error {
	tc.currentStatus = BEGIN
	txnState := TxnMetadata{
		State: BEGIN,
	}

	encoded, err := tc.payloadSerde.Encode(txnState)
	if err != nil {
		return err
	}
	msg_encoded, err := tc.msgSerde.Encode(nil, encoded)
	if err != nil {
		return err
	}
	_, err = tc.TransactionLog.Push(msg_encoded, 0, nil)
	return err
}

func (tc *TransactionManager) CommitTransaction() error {
	tc.currentStatus = PREPARE_COMMIT
	return nil
}

func (tc *TransactionManager) AbortTransaction() error {
	tc.currentStatus = PREPARE_ABORT
	return nil
}
