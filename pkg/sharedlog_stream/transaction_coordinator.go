package sharedlog_stream

import (
	"context"
	"encoding/json"

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
	Topic  string `json:"tp"`
	ParNum uint32 `json:"pn"`
}

type TxnState struct {
	state TransactionStatus `json:"st"`
}

type TransactionCoordinator struct {
	TransactionalId string
	TransactionLog  *SharedLogStream
	currentStatus   TransactionStatus
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
	encoded, err := json.Marshal(tp)
	if err != nil {
		return err
	}
	msg := processor.MessageSerialized{
		Value: encoded,
	}
	msg_encoded, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = tc.TransactionLog.Push(msg_encoded)
	return err
}

func (tc *TransactionCoordinator) BeginTransaction() error {
	tc.currentStatus = BEGIN
	txnState := TxnState{
		state: BEGIN,
	}
	encoded, err := json.Marshal(txnState)
	if err != nil {
		return err
	}
	msg := processor.MessageSerialized{
		Value: encoded,
	}
	msg_encoded, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	_, err = tc.TransactionLog.Push(msg_encoded)
	return err
}
