//go:generate greenpack
//msgp:ignore TransactionCoordinator
package sharedlog_stream

import (
	"context"

	"cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"
	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

const (
	TRANSACTION_LOG_TOPIC_NAME    = "__transaction_log"
	NUM_TRANSACTION_LOG_PARTITION = 50
)

type TransactionStatus uint8

const (
	BEGIN           TransactionStatus = 0
	PREPARE_COMMIT  TransactionStatus = 1
	PREPARE_ABORT   TransactionStatus = 2
	COMPLETE_COMMIT TransactionStatus = 3
	COMPLETE_ABORT  TransactionStatus = 4
)

type CommitMarker uint8

const (
	COMMIT CommitMarker = 0
	ABORT  CommitMarker = 1
)

type TopicPartition struct {
	Topic  string `zid:"0"`
	ParNum uint32 `zid:"1"`
}

type TxnState struct {
	State TransactionStatus `zid:"0"`
}

type TransactionCoordinator struct {
	TransactionalId string
	TransactionLog  *SharedLogStream
	currentStatus   TransactionStatus
	payloadSerde    processor.Serde
	msgSerde        processor.MsgSerde
}

func NewTransactionCoordinator(ctx context.Context, env types.Environment, transactional_id string) (*TransactionCoordinator, error) {
	log, err := NewSharedLogStream(ctx, env,
		TRANSACTION_LOG_TOPIC_NAME+"_"+transactional_id)
	if err != nil {
		return nil, err
	}
	return &TransactionCoordinator{
		TransactionLog:  log,
		TransactionalId: transactional_id,
	}, nil
}

func (tc *TransactionCoordinator) InitTransaction() {
	// Steps:
	// 1. roll forward/backward the transactions that are not finished
}

func (tc *TransactionCoordinator) RegisterTopicPartition(topic string, parNum uint32) error {
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
	_, err = tc.TransactionLog.Push(msg_encoded)
	return err
}

func (tc *TransactionCoordinator) BeginTransaction() error {
	tc.currentStatus = BEGIN
	txnState := TxnState{
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
	_, err = tc.TransactionLog.Push(msg_encoded)
	return err
}