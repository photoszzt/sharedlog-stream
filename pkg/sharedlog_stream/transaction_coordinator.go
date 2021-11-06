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

type TransactionManager struct {
	msgSerde        commtypes.MsgSerde
	txnMdSerde      commtypes.Serde
	tpSerde         commtypes.Serde
	txnMarkerSerde  commtypes.Serde
	TransactionLog  *SharedLogStream
	TransactionalId string
	currentStatus   TransactionState
}

func NewTransactionManager(ctx context.Context, env types.Environment, transactional_id string, serdeFormat commtypes.SerdeFormat) (*TransactionManager, error) {
	log, err := NewSharedLogStream(ctx, env,
		TRANSACTION_LOG_TOPIC_NAME+"_"+transactional_id)
	if err != nil {
		return nil, err
	}
	var msgSerde commtypes.MsgSerde
	var txnMdSerde commtypes.Serde
	var tpSerde commtypes.Serde
	var txnMarkerSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		msgSerde = commtypes.MessageSerializedJSONSerde{}
		txnMdSerde = TxnMetadataJSONSerde{}
		tpSerde = TopicPartitionJSONSerde{}
		txnMarkerSerde = TxnMarkerJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		msgSerde = commtypes.MessageSerializedMsgpSerde{}
		txnMdSerde = TxnMetadataMsgpSerde{}
		tpSerde = TopicPartitionMsgpSerde{}
		txnMarkerSerde = TxnMarkerMsgpSerde{}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return &TransactionManager{
		TransactionLog:  log,
		TransactionalId: transactional_id,
		msgSerde:        msgSerde,
		txnMdSerde:      txnMdSerde,
		currentStatus:   EMPTY,
		tpSerde:         tpSerde,
		txnMarkerSerde:  txnMarkerSerde,
	}, nil
}

func (tc *TransactionManager) InitTransaction() {
	// Steps:
	// 1. roll forward/backward the transactions that are not finished
}

func (tc *TransactionManager) appendToLog(transactionalId string, tm *TxnMetadata) error {
	encoded, err := tc.txnMdSerde.Encode(tm)
	if err != nil {
		return err
	}
	strSerde := commtypes.StringEncoder{}
	keyEncoded, err := strSerde.Encode(transactionalId)
	if err != nil {
		return err
	}
	msg_encoded, err := tc.msgSerde.Encode(keyEncoded, encoded)
	if err != nil {
		return err
	}
	_, err = tc.TransactionLog.Push(msg_encoded, 0, nil)
	return err
}

func (tc *TransactionManager) RegisterTopicPartitions(transactionalId string, appId uint64, appEpoch uint16, topicPartitions []TopicPartition) error {
	if tc.currentStatus != BEGIN {
		return xerrors.Errorf("should begin transaction first")
	}
	txnMd := TxnMetadata{
		AppId:           appId,
		AppEpoch:        appEpoch,
		TopicPartitions: topicPartitions,
		State:           tc.currentStatus,
	}
	err := tc.appendToLog(transactionalId, &txnMd)
	return err
}

func (tc *TransactionManager) BeginTransaction(transactionalId string) error {
	tc.currentStatus = BEGIN
	txnState := TxnMetadata{
		State: BEGIN,
	}
	return tc.appendToLog(transactionalId, &txnState)
}

func (tc *TransactionManager) CommitTransaction() error {
	tc.currentStatus = PREPARE_COMMIT
	return nil
}

func (tc *TransactionManager) AbortTransaction() error {
	tc.currentStatus = PREPARE_ABORT
	return nil
}
