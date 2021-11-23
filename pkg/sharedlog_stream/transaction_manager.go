package sharedlog_stream

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"

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
)

// each transaction manager manages one topic partition;
// assume each transactional_id correspond to one output partition
type TransactionManager struct {
	tmMu sync.RWMutex

	msgSerde              commtypes.MsgSerde
	txnMdSerde            commtypes.Serde
	tpSerde               commtypes.Serde
	txnMarkerSerde        commtypes.Serde
	offsetRecordSerde     commtypes.Serde
	backgroundJobCtx      context.Context
	topicStreams          map[string]store.Stream
	transactionLog        *SharedLogStream
	currentTopicPartition map[string]map[uint8]struct{}
	backgroundJobErrg     *errgroup.Group
	TransactionalId       string
	currentAppId          uint64 // 0 is NONE
	currentEpoch          uint16 // 0 is NONE
	currentStatus         TransactionState
}

func BeginTag(nameHash uint64, parNum uint8) uint64 {
	return nameHash<<(PartitionBits+LogTagReserveBits) + uint64(parNum)<<LogTagReserveBits + TransactionLogBegin
}

func FenceTag(nameHash uint64, parNum uint8) uint64 {
	return nameHash<<(PartitionBits+LogTagReserveBits) + uint64(parNum)<<LogTagReserveBits + TransactionLogFence
}

func NewTransactionManager(ctx context.Context, env types.Environment, transactional_id string, serdeFormat commtypes.SerdeFormat) (*TransactionManager, error) {
	log := NewSharedLogStream(env, TRANSACTION_LOG_TOPIC_NAME+"_"+transactional_id)
	/*
		err := log.InitStream(ctx, 0, true)
		if err != nil {
			return nil, err
		}
	*/
	errg, gctx := errgroup.WithContext(ctx)
	tm := &TransactionManager{
		transactionLog:        log,
		TransactionalId:       transactional_id,
		currentStatus:         EMPTY,
		currentTopicPartition: make(map[string]map[uint8]struct{}),
		topicStreams:          make(map[string]store.Stream),
		backgroundJobErrg:     errg,
		backgroundJobCtx:      gctx,
		currentEpoch:          0,
		currentAppId:          0,
	}
	err := tm.setupSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	return tm, nil
}

func (tc *TransactionManager) setupSerde(serdeFormat commtypes.SerdeFormat) error {
	if serdeFormat == commtypes.JSON {
		tc.msgSerde = commtypes.MessageSerializedJSONSerde{}
		tc.txnMdSerde = TxnMetadataJSONSerde{}
		tc.tpSerde = TopicPartitionJSONSerde{}
		tc.txnMarkerSerde = TxnMarkerJSONSerde{}
		tc.offsetRecordSerde = OffsetRecordJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		tc.msgSerde = commtypes.MessageSerializedMsgpSerde{}
		tc.txnMdSerde = TxnMetadataMsgpSerde{}
		tc.tpSerde = TopicPartitionMsgpSerde{}
		tc.txnMarkerSerde = TxnMarkerMsgpSerde{}
		tc.offsetRecordSerde = OffsetRecordMsgpSerde{}
	} else {
		return fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return nil
}

func (tc *TransactionManager) genAppId() {
	for tc.currentAppId == 0 {
		tc.currentAppId = tc.transactionLog.env.GenerateUniqueID()
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

	tmpStatus := EMPTY
	tmpAppID := uint64(0)
	tmpEpoch := uint16(0)

	// find the begin of the last transaction
	_, rawMsg, err := tc.transactionLog.readBackwardWithTag(ctx, 0, BeginTag(tc.transactionLog.topicNameHash, 0))
	if err != nil {
		// empty log
		if errors.IsStreamEmptyError(err) {
			return nil
		}
		return err
	}
	// empty log
	if rawMsg == nil {
		return nil
	}
	for {
		_, msgs, err := tc.transactionLog.ReadNext(ctx, 0)
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
			val, err := tc.txnMdSerde.Decode(valEncoded)
			if err != nil {
				return err
			}
			txnMeta := val.(TxnMetadata)
			// We have updated our app id; the fence will be the last entry in the transaction log
			if txnMeta.State != FENCE || (tmpAppID != tc.currentAppId && tmpEpoch != tc.currentEpoch) {
				tmpStatus = txnMeta.State
				tmpAppID = txnMeta.AppId
				tmpEpoch = txnMeta.AppEpoch
			}
			if txnMeta.TopicPartitions != nil {
				lastTopicPartitions = append(lastTopicPartitions, txnMeta.TopicPartitions...)
			}
		}
	}
	// check the last status of the transaction
	switch tmpStatus {
	case EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT:
		log.Info().Msgf("examed previous transaction with no error")
	case BEGIN:
		// need to abort
		currentStatus := tc.currentStatus
		currentAppId := tc.currentAppId
		currentEpoch := tc.currentEpoch

		// use the previous app id to finish the previous transaction
		tc.currentStatus = tmpStatus
		fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
		tc.currentAppId = tmpAppID
		tc.currentEpoch = tmpEpoch
		tc.loadCurrentTopicPartitions(lastTopicPartitions)
		err := tc.AbortTransaction(ctx)
		if err != nil {
			return err
		}
		// swap back
		tc.currentStatus = currentStatus
		fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
		tc.currentAppId = currentAppId
		tc.currentEpoch = currentEpoch
	case PREPARE_ABORT:
		// need to abort
		currentStatus := tc.currentStatus

		currentAppId := tc.currentAppId
		currentEpoch := tc.currentEpoch

		// use the previous app id to finish the previous transaction
		tc.currentStatus = tmpStatus
		fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
		tc.currentAppId = tmpAppID
		tc.currentEpoch = tmpEpoch

		// the transaction is aborted but the marker might not pushed to the relevant partitions yet
		tc.loadCurrentTopicPartitions(lastTopicPartitions)
		err := tc.completeTransaction(ctx, ABORT, COMPLETE_ABORT)
		if err != nil {
			return err
		}
		tc.cleanupState()
		// swap back
		tc.currentStatus = currentStatus
		fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
		tc.currentAppId = currentAppId
		tc.currentEpoch = currentEpoch
	case PREPARE_COMMIT:
		// the transaction is commited but the marker might not pushed to the relevant partitions yet
		// need to abort
		currentStatus := tc.currentStatus
		currentAppId := tc.currentAppId
		currentEpoch := tc.currentEpoch

		// use the previous app id to finish the previous transaction
		tc.currentStatus = tmpStatus
		fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
		tc.currentAppId = tmpAppID
		tc.currentEpoch = tmpEpoch

		tc.loadCurrentTopicPartitions(lastTopicPartitions)
		err := tc.completeTransaction(ctx, COMMIT, COMPLETE_COMMIT)
		if err != nil {
			return err
		}
		tc.cleanupState()

		// swap back
		tc.currentStatus = currentStatus
		fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
		tc.currentAppId = currentAppId
		tc.currentEpoch = currentEpoch
	case FENCE:
		// it's in a view change.
		log.Info().Msgf("Last operation in the log is fence to update the epoch. We are updating the epoch again.")
	}
	return nil
}

func (tc *TransactionManager) InitTransaction(ctx context.Context) (uint64, uint16, error) {
	// Steps:
	// fence first, to stop the zoombie instance from writing to the transaction log
	// clean up the log

	tc.tmMu.Lock()
	defer tc.tmMu.Unlock()

	tc.currentStatus = FENCE
	fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
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
	tags := []uint64{NameHashWithPartition(tc.transactionLog.topicNameHash, 0), FenceTag(tc.transactionLog.topicNameHash, 0)}
	err := tc.appendToTransactionLog(ctx, &txnMeta, tags)
	if err != nil {
		return 0, 0, fmt.Errorf("appendToTransactionLog failed: %v", err)
	}
	err = tc.loadTransactionFromLog(ctx)
	if err != nil {
		return 0, 0, fmt.Errorf("loadTransactinoFromLog failed: %v", err)
	}

	return tc.currentAppId, tc.currentEpoch, nil
}

func (tc *TransactionManager) MonitorTransactionLog(ctx context.Context, quit chan struct{}, errc chan error, dcancel context.CancelFunc) {
	fenceTag := FenceTag(tc.transactionLog.topicNameHash, 0)
	strSerde := commtypes.StringSerde{}
	for {
		select {
		case <-quit:
			return
		default:
		}
		_, rawMsgs, err := tc.transactionLog.ReadNextWithTag(ctx, 0, fenceTag)
		if err != nil {
			if errors.IsStreamEmptyError(err) {
				continue
			}
			errc <- err
		} else {
			for _, rawMsg := range rawMsgs {
				keyBytes, valBytes, err := tc.msgSerde.Decode(rawMsg.Payload)
				if err != nil {
					errc <- err
					break
				}
				trStr, err := strSerde.Decode(keyBytes)
				if err != nil {
					errc <- err
					break
				}
				transactionalId := trStr.(string)
				// not the fence it's looking at
				if transactionalId != tc.TransactionalId {
					continue
				}
				txnMetaTmp, err := tc.txnMdSerde.Decode(valBytes)
				if err != nil {
					errc <- err
					break
				}
				txnMeta := txnMetaTmp.(TxnMetadata)
				if txnMeta.State != FENCE {
					panic("fence state should be fence")
				}
				if txnMeta.AppId == tc.currentAppId && txnMeta.AppEpoch > tc.currentEpoch {
					// I'm the zoombie
					dcancel()
					errc <- nil
					break
				}
			}
		}
	}
}

func (tc *TransactionManager) appendToTransactionLog(ctx context.Context, tm *TxnMetadata, tags []uint64) error {
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
	if tags != nil {
		_, err = tc.transactionLog.PushWithTag(ctx, msg_encoded, 0, tags, false)
	} else {
		_, err = tc.transactionLog.Push(ctx, msg_encoded, 0, false)
	}
	return err
}

func (tc *TransactionManager) appendTxnMarkerToStreams(ctx context.Context, marker TxnMark, appId uint64, appEpoch uint16) error {
	tm := TxnMarker{
		Mark: uint8(marker),
	}
	encoded, err := tc.txnMarkerSerde.Encode(&tm)
	if err != nil {
		return err
	}
	msg_encoded, err := tc.msgSerde.Encode(nil, encoded)
	if err != nil {
		return err
	}
	g, ctx := errgroup.WithContext(ctx)
	for topic, partitions := range tc.currentTopicPartition {
		stream := tc.topicStreams[topic]
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
	err := tc.appendToTransactionLog(ctx, &txnMd, nil)
	return err
}

func (tc *TransactionManager) AddTopicPartition(ctx context.Context, topic string, partitions []uint8) error {
	tc.tmMu.Lock()
	defer tc.tmMu.Unlock()

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
		return tc.appendToTransactionLog(ctx, &txnMd, nil)
	}
	return nil
}

func (tc *TransactionManager) CreateOffsetTopic(topicToTrack string, numPartition uint8) error {
	tc.tmMu.Lock()
	defer tc.tmMu.Unlock()

	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	_, ok := tc.topicStreams[offsetTopic]
	if ok {
		// already exists
		return nil
	}
	off, err := NewShardedSharedLogStream(tc.transactionLog.env, offsetTopic, numPartition)
	if err != nil {
		return err
	}
	tc.topicStreams[offsetTopic] = off
	return nil
}

func (tc *TransactionManager) RecordTopicStreams(topicToTrack string, stream store.Stream) {
	tc.tmMu.Lock()
	defer tc.tmMu.Unlock()

	_, ok := tc.topicStreams[topicToTrack]
	if ok {
		return
	}
	tc.topicStreams[topicToTrack] = stream
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
	tc.tmMu.RLock()
	defer tc.tmMu.RUnlock()

	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + offsetConfig.TopicToTrack
	offsetLog := tc.topicStreams[offsetTopic]
	offsetRecord := OffsetRecord{
		Offset:   offsetConfig.Offset,
		AppId:    offsetConfig.AppId,
		AppEpoch: offsetConfig.AppEpoch,
	}
	encoded, err := tc.offsetRecordSerde.Encode(&offsetRecord)
	if err != nil {
		return err
	}
	_, err = offsetLog.Push(ctx, encoded, offsetConfig.Partition, false)
	return err
}

func (tc *TransactionManager) BeginTransaction(ctx context.Context) error {
	tc.tmMu.Lock()
	defer tc.tmMu.Unlock()
	if !BEGIN.IsValidPreviousState(tc.currentStatus) {
		fmt.Fprintf(os.Stderr, "current state is %d\n", tc.currentStatus)
		return errors.ErrInvalidStateTransition
	}
	tc.currentStatus = BEGIN
	fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)

	txnState := TxnMetadata{
		State:    tc.currentStatus,
		AppId:    tc.currentAppId,
		AppEpoch: tc.currentEpoch,
	}
	tags := []uint64{NameHashWithPartition(tc.transactionLog.topicNameHash, 0), BeginTag(tc.transactionLog.topicNameHash, 0)}
	return tc.appendToTransactionLog(ctx, &txnState, tags)
}

// second phase of the 2-phase commit protocol
func (tc *TransactionManager) completeTransaction(ctx context.Context, trMark TxnMark, trState TransactionState) error {
	// append txn marker to all topic partitions
	err := tc.appendTxnMarkerToStreams(ctx, trMark, tc.currentAppId, tc.currentEpoch)
	if err != nil {
		return err
	}
	// async append complete_commit
	tc.currentStatus = trState
	fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	txnMd := TxnMetadata{
		State: tc.currentStatus,
	}
	tc.backgroundJobErrg.Go(func() error {
		return tc.appendToTransactionLog(tc.backgroundJobCtx, &txnMd, nil)
	})
	return nil
}

func (tc *TransactionManager) CommitTransaction(ctx context.Context) error {
	tc.tmMu.Lock()
	defer tc.tmMu.Unlock()
	if !PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		fmt.Fprintf(os.Stderr, "Fail to transition from %s to PREPARE_COMMIT\n", tc.currentStatus.String())
		return errors.ErrInvalidStateTransition
	}

	// first phase of the commit
	tc.currentStatus = PREPARE_COMMIT
	fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	err := tc.registerTopicPartitions(ctx)
	if err != nil {
		return err
	}

	// second phase of the commit
	err = tc.completeTransaction(ctx, COMMIT, COMPLETE_COMMIT)
	if err != nil {
		return err
	}
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) AbortTransaction(ctx context.Context) error {
	tc.tmMu.Lock()
	defer tc.tmMu.Unlock()
	if !PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		return errors.ErrInvalidStateTransition
	}
	tc.currentStatus = PREPARE_ABORT
	fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	err := tc.registerTopicPartitions(ctx)
	if err != nil {
		return err
	}

	err = tc.completeTransaction(ctx, ABORT, COMPLETE_ABORT)
	if err != nil {
		return err
	}
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) cleanupState() {
	tc.currentStatus = EMPTY
	fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	tc.currentTopicPartition = make(map[string]map[uint8]struct{})
}

func (tc *TransactionManager) Close() error {
	// wait for all background go rountine to finish
	return tc.backgroundJobErrg.Wait()
}
