package sharedlog_stream

import (
	"context"
	"fmt"

	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/sync/errgroup"
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
	ctx                   context.Context // background ctx
	TransactionLog        *SharedLogStream
	currentTopicPartition map[string][]uint8
	errg                  *errgroup.Group // err group for background tasks
	TransactionalId       string
	currentStatus         TransactionState
	currentEpoch          uint16
}

type StreamPartition struct {
	Stream store.Stream
	ParNum uint8
}

func NewTransactionManager(ctx context.Context, env types.Environment, transactional_id string, serdeFormat commtypes.SerdeFormat) (*TransactionManager, error) {
	log := NewSharedLogStream(env, TRANSACTION_LOG_TOPIC_NAME+"_"+transactional_id)
	err := log.InitStream(ctx)
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
	errg, gctx := errgroup.WithContext(ctx)
	return &TransactionManager{
		TransactionLog:        log,
		TransactionalId:       transactional_id,
		msgSerde:              msgSerde,
		txnMdSerde:            txnMdSerde,
		currentStatus:         EMPTY,
		tpSerde:               tpSerde,
		txnMarkerSerde:        txnMarkerSerde,
		currentTopicPartition: make(map[string][]uint8),
		errg:                  errg,
		ctx:                   gctx,
	}, nil
}

func (tc *TransactionManager) InitTransaction() (uint64, uint16) {
	// Steps:
	// 1. roll forward/backward the transactions that are not finished
	// 2. generate app id and epoch number
	appId := tc.TransactionLog.env.GenerateUniqueID()
	return appId, tc.currentEpoch
}

func (tc *TransactionManager) appendToTransactionLog(ctx context.Context, tm *TxnMetadata) error {
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
	_, err = tc.TransactionLog.Push(ctx, msg_encoded, 0, nil)
	return err
}

func (tc *TransactionManager) appendTxnMarkerToStreams(ctx context.Context, marker TxnMark, appId uint64, appEpoch uint16, sps []StreamPartition) error {
	tm := TxnMarker{
		Mark:     uint8(marker),
		AppEpoch: appEpoch,
		AppId:    appId,
	}
	encoded, err := tc.txnMarkerSerde.Encode(tm)
	if err != nil {
		return err
	}
	msg_encoded, err := tc.msgSerde.Encode(nil, encoded)
	if err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)
	for _, sp := range sps {
		streamPar := sp
		g.Go(func() error {
			_, err = streamPar.Stream.Push(ctx, msg_encoded, streamPar.ParNum, nil)
			return err
		})
	}
	return g.Wait()
}

func (tc *TransactionManager) registerTopicPartitions(ctx context.Context, appId uint64, appEpoch uint16) error {
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
	err := tc.appendToTransactionLog(ctx, &txnMd)
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

func (tc *TransactionManager) BeginTransaction(ctx context.Context, appId uint64, appEpoch uint16) error {
	if !BEGIN.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}
	tc.currentStatus = BEGIN
	txnState := TxnMetadata{
		State:    tc.currentStatus,
		AppId:    appId,
		AppEpoch: appEpoch,
	}
	return tc.appendToTransactionLog(ctx, &txnState)
}

func (tc *TransactionManager) CommitTransaction(ctx context.Context, appId uint64, appEpoch uint16, streams []StreamPartition) error {
	if !PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}

	tc.currentStatus = PREPARE_COMMIT
	err := tc.registerTopicPartitions(ctx, appId, appEpoch)
	if err != nil {
		return err
	}

	// append txn marker to all topic partitions
	err = tc.appendTxnMarkerToStreams(ctx, COMMIT, appId, appEpoch, streams)
	if err != nil {
		return err
	}
	// async append complete_commit
	tc.currentStatus = COMPLETE_COMMIT
	txnMd := TxnMetadata{
		State:    tc.currentStatus,
		AppId:    appId,
		AppEpoch: appEpoch,
	}
	tc.errg.Go(func() error {
		return tc.appendToTransactionLog(tc.ctx, &txnMd)
	})
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) AbortTransaction(ctx context.Context, appId uint64, appEpoch uint16, streams []StreamPartition) error {
	if !PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}
	tc.currentStatus = PREPARE_ABORT
	err := tc.registerTopicPartitions(ctx, appId, appEpoch)
	if err != nil {
		return err
	}

	// append txn mark to all topic partitions
	err = tc.appendTxnMarkerToStreams(ctx, ABORT, appId, appEpoch, streams)
	if err != nil {
		return err
	}
	// async append complete_abort
	tc.currentStatus = COMPLETE_ABORT
	txnMd := TxnMetadata{
		State:    tc.currentStatus,
		AppId:    appId,
		AppEpoch: appEpoch,
	}
	tc.errg.Go(func() error {
		return tc.appendToTransactionLog(tc.ctx, &txnMd)
	})
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) cleanupState() {
	tc.currentStatus = EMPTY
	tc.currentTopicPartition = make(map[string][]uint8)
}

func (tc *TransactionManager) Close() error {
	// wait for all background go rountine to finish
	return tc.errg.Wait()
}
