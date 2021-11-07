package sharedlog_stream

import (
	"context"
	"fmt"

	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

const (
	TRANSACTION_LOG_TOPIC_NAME     = "__transaction_log"
	CONSUMER_OFFSET_LOG_TOPIC_NAME = "__offset_log"
	NUM_TRANSACTION_LOG_PARTITION  = 50
)

var (
	ErrInvalidStateTransition = xerrors.New("invalid state transition")
)

type TransactionManager struct {
	msgSerde              commtypes.MsgSerde
	txnMdSerde            commtypes.Serde
	tpSerde               commtypes.Serde
	txnMarkerSerde        commtypes.Serde
	TransactionLog        *SharedLogStream
	currentTopicPartition map[string][]uint8
	TransactionalId       string
	currentStatus         TransactionState
}

type StreamPartition struct {
	Stream store.Stream
	ParNum uint8
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
		TransactionLog:        log,
		TransactionalId:       transactional_id,
		msgSerde:              msgSerde,
		txnMdSerde:            txnMdSerde,
		currentStatus:         EMPTY,
		tpSerde:               tpSerde,
		txnMarkerSerde:        txnMarkerSerde,
		currentTopicPartition: make(map[string][]uint8),
	}, nil
}

func (tc *TransactionManager) InitTransaction() {
	// Steps:
	// 1. roll forward/backward the transactions that are not finished
}

func (tc *TransactionManager) appendToTransactionLog(tm *TxnMetadata) error {
	encoded, err := tc.txnMdSerde.Encode(tm)
	if err != nil {
		return err
	}
	strSerde := commtypes.StringEncoder{}
	keyEncoded, err := strSerde.Encode(tc.TransactionalId)
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

func (tc *TransactionManager) appendTxnMarkerToStream(tm *TxnMarker, sp *StreamPartition) error {
	encoded, err := tc.txnMarkerSerde.Encode(tm)
	if err != nil {
		return err
	}
	msg_encoded, err := tc.msgSerde.Encode(nil, encoded)
	if err != nil {
		return err
	}
	_, err = sp.Stream.Push(msg_encoded, sp.ParNum, nil)
	return err
}

func (tc *TransactionManager) appendTxnMarkerToStreams(marker TxnMark, appId uint64, appEpoch uint16, sps []StreamPartition) {
	tm := TxnMarker{
		Mark:     uint8(marker),
		AppEpoch: appEpoch,
		AppId:    appId,
	}
	for sp := range sps {

	}
}

func (tc *TransactionManager) registerTopicPartitions(appId uint64, appEpoch uint16) error {
	if tc.currentStatus != BEGIN {
		return xerrors.Errorf("should begin transaction first")
	}
	var tps []TopicPartition
	for topic, pars := range tc.currentTopicPartition {
		tp := TopicPartition{
			Topic:  topic,
			ParNum: pars,
		}
		tps = append(tps, tp)
	}
	txnMd := TxnMetadata{
		AppId:           appId,
		AppEpoch:        appEpoch,
		TopicPartitions: tps,
		State:           tc.currentStatus,
	}
	err := tc.appendToTransactionLog(&txnMd)
	return err
}

func (tc *TransactionManager) AddTopicPartition(topic string, partition uint8) error {
	if tc.currentStatus != BEGIN {
		return xerrors.Errorf("should begin transaction first")
	}
	par, ok := tc.currentTopicPartition[topic]
	if !ok {
		par = make([]uint8, 0)
	}
	par = append(par, partition)
	tc.currentTopicPartition[topic] = par
	return nil
}

func (tc *TransactionManager) BeginTransaction(appId uint64, appEpoch uint16) error {
	if !BEGIN.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}
	tc.currentStatus = BEGIN
	txnState := TxnMetadata{
		State:    tc.currentStatus,
		AppId:    appId,
		AppEpoch: appEpoch,
	}
	return tc.appendToTransactionLog(&txnState)
}

func (tc *TransactionManager) CommitTransaction(appId uint64, appEpoch uint16, streams []StreamPartition) error {
	if !PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}

	tc.currentStatus = PREPARE_COMMIT
	err := tc.registerTopicPartitions(appId, appEpoch)
	if err != nil {
		return err
	}

	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) AbortTransaction(appId uint64, appEpoch uint16, streams []StreamPartition) error {
	if !PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}
	tc.currentStatus = PREPARE_ABORT
	err := tc.registerTopicPartitions(appId, appEpoch)
	if err != nil {
		return err
	}

	// append
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) cleanupState() {
	tc.currentStatus = EMPTY
	tc.currentTopicPartition = make(map[string][]uint8)
}
