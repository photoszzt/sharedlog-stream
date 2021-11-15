package sharedlog_stream

import (
	"context"
	"fmt"
	"math"

	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
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
	backgroundJobCtx      context.Context
	TopicStreams          map[string]store.Stream
	TransactionLog        *SharedLogStream
	currentTopicPartition map[string]map[uint8]struct{}
	backgroundJobErrg     *errgroup.Group
	// assume there's only one transaction id for a application
	TransactionalId string
	currentAppId    uint64 // 0 is NONE
	currentEpoch    uint16 // 0 is NONE
	currentStatus   TransactionState
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
		backgroundJobErrg:     errg,
		backgroundJobCtx:      gctx,
		currentEpoch:          0,
		currentAppId:          0,
	}, nil
}

func (tc *TransactionManager) genAppId() {
	for tc.currentAppId == 0 {
		tc.currentAppId = tc.TransactionLog.env.GenerateUniqueID()
	}
}

func (tc *TransactionManager) loadCurrentTopicPartitions(lastTopicPartitions []TopicPartition) {
	for _, tp := range lastTopicPartitions {
		pars, ok := tc.currentTopicPartition[tp.Topic]
		if !ok {
			pars = make(map[uint8]struct{})
		}
		for _, par := range tp.ParNum {
			pars[par] = struct{}{}
		}
	}
}

// each transaction id corresponds to a separate transaction log; we only have one transaction id per serverless function
func (tc *TransactionManager) loadTransactionFromLog(ctx context.Context) error {
	strSerde := commtypes.StringSerde{}
	var lastTopicPartitions []TopicPartition
	for {
		_, msgs, err := tc.TransactionLog.ReadNext(ctx, 0)
		if errors.IsStreamEmptyError(err) {
			break
		} else if err != nil {
			return err
		}
		for _, msg := range msgs {
			keyEncoded, valEncoded, err := tc.msgSerde.Decode(msg.Payload)
			if err != nil {
				return err
			}
			key, err := strSerde.Decode(keyEncoded)
			if err != nil {
				return err
			}
			tc.TransactionalId = key.(string)
			val, err := tc.txnMarkerSerde.Decode(valEncoded)
			if err != nil {
				return err
			}
			txnMeta := val.(TxnMetadata)
			tc.currentStatus = txnMeta.State
			tc.currentAppId = txnMeta.AppId
			tc.currentEpoch = txnMeta.AppEpoch
			if txnMeta.TopicPartitions != nil {
				lastTopicPartitions = txnMeta.TopicPartitions
			}
		}
	}
	// check the last status of the transaction
	switch tc.currentStatus {
	case EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT:
		log.Info().Msgf("examed all previous transaction with no error")
	case BEGIN:
		// need to abort
		err := tc.AbortTransaction(ctx, tc.currentAppId, tc.currentEpoch)
		if err != nil {
			return err
		}
	case PREPARE_ABORT:
		// the transaction is aborted but the marker might not pushed to the relevant partitions yet
		tc.loadCurrentTopicPartitions(lastTopicPartitions)
		err := tc.completeTransaction(ctx, tc.currentAppId, tc.currentEpoch, ABORT, COMPLETE_ABORT)
		if err != nil {
			return err
		}
		tc.cleanupState()
	case PREPARE_COMMIT:
		// the transaction is commited but the marker might not pushed to the relevant partitions yet
		tc.loadCurrentTopicPartitions(lastTopicPartitions)
		err := tc.completeTransaction(ctx, tc.currentAppId, tc.currentEpoch, COMMIT, COMPLETE_COMMIT)
		if err != nil {
			return err
		}
		tc.cleanupState()
	case FENCE:
		// it's in a view change.
		log.Info().Msgf("Last operation in the log is fence to update the epoch. We are updating the epoch again.")
	}
	return nil
}

func (tc *TransactionManager) InitTransaction(ctx context.Context) (uint64, uint16, error) {
	// Steps:
	// 1. roll forward/backward the transactions that are not finished
	// 2. generate app id and epoch number
	tc.currentStatus = FENCE
	err := tc.loadTransactionFromLog(ctx)
	if err != nil {
		return 0, 0, err
	}
	if tc.currentAppId == 0 {
		tc.genAppId()
	}
	if tc.currentEpoch == math.MaxUint16 {
		tc.genAppId()
		tc.currentEpoch = 0
	}
	tc.currentEpoch += 1
	txnMeta := TxnMetadata{
		AppId:    tc.currentAppId,
		AppEpoch: tc.currentEpoch,
		State:    tc.currentStatus,
	}
	err = tc.appendToTransactionLog(ctx, &txnMeta)
	if err != nil {
		return 0, 0, err
	}
	return tc.currentAppId, tc.currentEpoch, nil
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
	_, err = tc.TransactionLog.Push(ctx, msg_encoded, 0, false)
	return err
}

func (tc *TransactionManager) appendTxnMarkerToStreams(ctx context.Context, marker TxnMark, appId uint64, appEpoch uint16) error {
	tm := TxnMarker{
		Mark: uint8(marker),
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
				_, err = stream.Push(ctx, msg_encoded, parNum, true)
				return err
			})
		}
	}
	return g.Wait()
}

func (tc *TransactionManager) registerTopicPartitions(ctx context.Context) error {
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
		TopicPartitions: tps,
		State:           tc.currentStatus,
		AppId:           tc.currentAppId,
		AppEpoch:        tc.currentEpoch,
	}
	err := tc.appendToTransactionLog(ctx, &txnMd)
	return err
}

func (tc *TransactionManager) AddTopicPartition(ctx context.Context, topic string, partitions []uint8) error {
	if tc.currentStatus != BEGIN {
		return xerrors.Errorf("should begin transaction first")
	}
	needToAppendToLog := false
	parSet, ok := tc.currentTopicPartition[topic]
	if !ok {
		parSet = make(map[uint8]struct{})
		needToAppendToLog = true
	}
	for _, parNum := range partitions {
		_, ok = parSet[parNum]
		if !ok {
			needToAppendToLog = true
			parSet[parNum] = struct{}{}
		}
	}
	tc.currentTopicPartition[topic] = parSet
	if needToAppendToLog {
		txnMd := TxnMetadata{
			TopicPartitions: []TopicPartition{{Topic: topic, ParNum: partitions}},
			State:           tc.currentStatus,
			AppId:           tc.currentAppId,
			AppEpoch:        tc.currentEpoch,
		}
		return tc.appendToTransactionLog(ctx, &txnMd)
	}
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

func (tc *TransactionManager) AddOffsets(ctx context.Context, topicToTrack string, partitions []uint8) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	return tc.AddTopicPartition(ctx, offsetTopic, partitions)
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
	_, err = offsetLog.Push(ctx, encoded, offsetConfig.Partition, false)
	return err
}

func (tc *TransactionManager) BeginTransaction(ctx context.Context, appId uint64, appEpoch uint16) error {
	if !BEGIN.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}
	tc.currentStatus = BEGIN
	txnState := TxnMetadata{
		State: tc.currentStatus,
	}
	return tc.appendToTransactionLog(ctx, &txnState)
}

// second phase of the 2-phase commit protocol
func (tc *TransactionManager) completeTransaction(ctx context.Context, appId uint64, appEpoch uint16, trMark TxnMark, trState TransactionState) error {
	// append txn marker to all topic partitions
	err := tc.appendTxnMarkerToStreams(ctx, trMark, appId, appEpoch)
	if err != nil {
		return err
	}
	// async append complete_commit
	tc.currentStatus = trState
	txnMd := TxnMetadata{
		State: tc.currentStatus,
	}
	tc.backgroundJobErrg.Go(func() error {
		return tc.appendToTransactionLog(tc.backgroundJobCtx, &txnMd)
	})
	return nil
}

func (tc *TransactionManager) CommitTransaction(ctx context.Context, appId uint64, appEpoch uint16) error {
	if !PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}

	// first phase of the commit
	tc.currentStatus = PREPARE_COMMIT
	err := tc.registerTopicPartitions(ctx)
	if err != nil {
		return err
	}

	// second phase of the commit
	err = tc.completeTransaction(ctx, appId, appEpoch, COMMIT, COMPLETE_COMMIT)
	if err != nil {
		return err
	}
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) AbortTransaction(ctx context.Context, appId uint64, appEpoch uint16) error {
	if !PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		return ErrInvalidStateTransition
	}
	tc.currentStatus = PREPARE_ABORT
	err := tc.registerTopicPartitions(ctx)
	if err != nil {
		return err
	}

	err = tc.completeTransaction(ctx, appId, appEpoch, ABORT, COMPLETE_ABORT)
	if err != nil {
		return err
	}
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) cleanupState() {
	tc.currentStatus = EMPTY
	tc.currentTopicPartition = make(map[string]map[uint8]struct{})
}

func (tc *TransactionManager) Close() error {
	// wait for all background go rountine to finish
	return tc.backgroundJobErrg.Wait()
}
