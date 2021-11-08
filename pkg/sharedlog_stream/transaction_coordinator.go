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
	offsetRecordSerde     commtypes.Serde
	ctx                   context.Context // background ctx
	TransactionLog        *SharedLogStream
	TopicStreams          map[string]store.Stream
	currentTopicPartition map[string]map[uint8]struct{}
	errg                  *errgroup.Group // err group for background tasks
	TransactionalId       string
	currentStatus         TransactionState
	currentEpoch          uint16
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
	var offsetRecordSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		msgSerde = commtypes.MessageSerializedJSONSerde{}
		txnMdSerde = TxnMetadataJSONSerde{}
		tpSerde = TopicPartitionJSONSerde{}
		txnMarkerSerde = TxnMarkerJSONSerde{}
		offsetRecordSerde = OffsetRecordJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		msgSerde = commtypes.MessageSerializedMsgpSerde{}
		txnMdSerde = TxnMetadataMsgpSerde{}
		tpSerde = TopicPartitionMsgpSerde{}
		txnMarkerSerde = TxnMarkerMsgpSerde{}
		offsetRecordSerde = OffsetRecordMsgpSerde{}
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
		offsetRecordSerde:     offsetRecordSerde,
		currentTopicPartition: make(map[string]map[uint8]struct{}),
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

func (tc *TransactionManager) appendTxnMarkerToStreams(ctx context.Context, marker TxnMark, appId uint64, appEpoch uint16) error {
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
	for topic, partitions := range tc.currentTopicPartition {
		stream := tc.TopicStreams[topic]
		for par := range partitions {
			parNum := par
			g.Go(func() error {
				_, err = stream.Push(ctx, msg_encoded, parNum, nil)
				return err
			})
		}
	}
	return g.Wait()
}

func (tc *TransactionManager) registerTopicPartitions(ctx context.Context, appId uint64, appEpoch uint16) error {
	if tc.currentStatus != BEGIN {
		return xerrors.Errorf("should begin transaction first")
	}
	var tps []TopicPartition
	for topic, pars := range tc.currentTopicPartition {
		pars_arr := make([]uint8, 0, len(pars))
		for p := range pars {
			pars_arr = append(pars_arr, p)
		}
		tp := TopicPartition{
			Topic:  topic,
			ParNum: pars_arr,
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
		par = make(map[uint8]struct{})
	}
	par[partition] = struct{}{}
	tc.currentTopicPartition[topic] = par
	return nil
}

func (tc *TransactionManager) CreateOffsetTopic(topicToTrack string, numPartition uint8) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	_, ok := tc.TopicStreams[offsetTopic]
	if ok {
		// already exists
		return nil
	}
	off, err := NewShardedSharedLogStream(tc.TransactionLog.env, offsetTopic, numPartition)
	if err != nil {
		return err
	}
	tc.TopicStreams[offsetTopic] = off
	return nil
}

func (tc *TransactionManager) RecordTopicStreams(topicToTrack string, stream store.Stream) {
	_, ok := tc.TopicStreams[topicToTrack]
	if ok {
		return
	}
	tc.TopicStreams[topicToTrack] = stream
}

func (tc *TransactionManager) AddOffsets(topicToTrack string, partition uint8) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	return tc.AddTopicPartition(offsetTopic, partition)
}

type OffsetConfig struct {
	TopicToTrack string
	AppId        uint64
	Offset       uint64
	AppEpoch     uint16
	Partition    uint8
}

func (tc *TransactionManager) AppendOffset(ctx context.Context, offsetConfig OffsetConfig) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + offsetConfig.TopicToTrack
	offsetLog := tc.TopicStreams[offsetTopic]
	offsetRecord := OffsetRecord{
		Offset:   offsetConfig.Offset,
		AppId:    offsetConfig.AppId,
		AppEpoch: offsetConfig.AppEpoch,
	}
	encoded, err := tc.offsetRecordSerde.Encode(offsetRecord)
	if err != nil {
		return err
	}
	_, err = offsetLog.Push(ctx, encoded, offsetConfig.Partition, nil)
	return err
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

func (tc *TransactionManager) CommitTransaction(ctx context.Context, appId uint64, appEpoch uint16) error {
	if !PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}

	tc.currentStatus = PREPARE_COMMIT
	err := tc.registerTopicPartitions(ctx, appId, appEpoch)
	if err != nil {
		return err
	}

	// append txn marker to all topic partitions
	err = tc.appendTxnMarkerToStreams(ctx, COMMIT, appId, appEpoch)
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

func (tc *TransactionManager) AbortTransaction(ctx context.Context, appId uint64, appEpoch uint16) error {
	if !PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}
	tc.currentStatus = PREPARE_ABORT
	err := tc.registerTopicPartitions(ctx, appId, appEpoch)
	if err != nil {
		return err
	}

	// append txn mark to all topic partitions
	err = tc.appendTxnMarkerToStreams(ctx, ABORT, appId, appEpoch)
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
	tc.currentTopicPartition = make(map[string]map[uint8]struct{})
}

func (tc *TransactionManager) Close() error {
	// wait for all background go rountine to finish
	return tc.errg.Wait()
}
