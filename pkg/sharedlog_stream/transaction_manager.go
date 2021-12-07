package sharedlog_stream

import (
	"context"
	"fmt"
	"math"

	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"cs.utexas.edu/zjia/faas/protocol"
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
	CurrentTaskId         uint64 // 0 is NONE
	CurrentEpoch          uint16 // 0 is NONE
	currentStatus         TransactionState
}

func BeginTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + TransactionLogBegin
}

func FenceTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + TransactionLogFence
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
		CurrentEpoch:          0,
		CurrentTaskId:         0,
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
	for tc.CurrentTaskId == 0 {
		tc.CurrentTaskId = tc.transactionLog.env.GenerateUniqueID()
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

func (tc *TransactionManager) getMostRecentTransactionState(ctx context.Context) (*TxnMetadata, error) {
	// fmt.Fprintf(os.Stderr, "load transaction log\n")
	mostRecentTxnMetadata := &TxnMetadata{
		TopicPartitions: make([]TopicPartition, 0),
		TaskId:          0,
		TaskEpoch:       0,
		State:           EMPTY,
	}

	// find the begin of the last transaction
	for {
		_, rawMsg, err := tc.transactionLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, 0, BeginTag(tc.transactionLog.topicNameHash, 0))
		if err != nil {
			// empty log
			if errors.IsStreamEmptyError(err) {
				return nil, nil
			}
			return nil, err
		}
		// empty log
		if rawMsg == nil {
			return nil, nil
		}
		_, valEncoded, err := tc.msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, err
		}
		val, err := tc.txnMdSerde.Decode(valEncoded)
		if err != nil {
			return nil, err
		}
		txnMeta := val.(TxnMetadata)
		if txnMeta.State != BEGIN {
			continue
		} else {
			// read from the begin of the last transaction
			tc.transactionLog.cursor = rawMsg.LogSeqNum
			break
		}
	}

	for {
		_, msgs, err := tc.transactionLog.ReadNext(ctx, 0)
		if errors.IsStreamEmptyError(err) {
			break
		} else if err != nil {
			return nil, err
		}
		for _, msg := range msgs {
			_, valEncoded, err := tc.msgSerde.Decode(msg.Payload)
			if err != nil {
				return nil, err
			}
			val, err := tc.txnMdSerde.Decode(valEncoded)
			if err != nil {
				return nil, err
			}
			txnMeta := val.(TxnMetadata)

			mostRecentTxnMetadata.State = txnMeta.State
			mostRecentTxnMetadata.TaskId = txnMeta.TaskId
			mostRecentTxnMetadata.TaskEpoch = txnMeta.TaskEpoch

			if txnMeta.TopicPartitions != nil {
				mostRecentTxnMetadata.TopicPartitions = append(mostRecentTxnMetadata.TopicPartitions, txnMeta.TopicPartitions...)
			}
		}
	}
	return mostRecentTxnMetadata, nil
}

// each transaction id corresponds to a separate transaction log; we only have one transaction id per serverless function
func (tc *TransactionManager) loadAndFixTransaction(ctx context.Context, mostRecentTxnMetadata *TxnMetadata) error {
	// check the last status of the transaction
	switch mostRecentTxnMetadata.State {
	case EMPTY, COMPLETE_COMMIT, COMPLETE_ABORT:
		log.Info().Msgf("examed previous transaction with no error")
	case BEGIN:
		// need to abort
		currentStatus := tc.currentStatus

		// use the previous app id to finish the previous transaction
		tc.currentStatus = mostRecentTxnMetadata.State
		// fmt.Fprintf(os.Stderr, "In repair: Transition to %s to restore\n", tc.currentStatus)

		// fmt.Fprintf(os.Stderr, "before load current topic partitions\n")
		tc.loadCurrentTopicPartitions(mostRecentTxnMetadata.TopicPartitions)
		// fmt.Fprintf(os.Stderr, "after load current topic partitions\n")
		err := tc.AbortTransaction(ctx)
		// fmt.Fprintf(os.Stderr, "after abort transactions\n")
		if err != nil {
			return err
		}
		// swap back
		tc.currentStatus = currentStatus
		// fmt.Fprintf(os.Stderr, "In repair: Transition back to %s\n", tc.currentStatus)
	case PREPARE_ABORT:
		// need to abort
		currentStatus := tc.currentStatus

		// use the previous app id to finish the previous transaction
		tc.currentStatus = mostRecentTxnMetadata.State
		// fmt.Fprintf(os.Stderr, "In repair: Transition to %s to restore\n", tc.currentStatus)

		// the transaction is aborted but the marker might not pushed to the relevant partitions yet
		tc.loadCurrentTopicPartitions(mostRecentTxnMetadata.TopicPartitions)
		err := tc.completeTransaction(ctx, ABORT, COMPLETE_ABORT)
		if err != nil {
			return err
		}
		tc.cleanupState()
		// swap back
		tc.currentStatus = currentStatus
		// fmt.Fprintf(os.Stderr, "In repair: Transition back to %s\n", tc.currentStatus)
	case PREPARE_COMMIT:
		// the transaction is commited but the marker might not pushed to the relevant partitions yet
		// need to abort
		currentStatus := tc.currentStatus

		// use the previous app id to finish the previous transaction
		tc.currentStatus = mostRecentTxnMetadata.State
		// fmt.Fprintf(os.Stderr, "In repair: Transition to %s to restore\n", tc.currentStatus)

		tc.loadCurrentTopicPartitions(mostRecentTxnMetadata.TopicPartitions)
		err := tc.completeTransaction(ctx, COMMIT, COMPLETE_COMMIT)
		if err != nil {
			return err
		}
		tc.cleanupState()

		// swap back
		tc.currentStatus = currentStatus
		// fmt.Fprintf(os.Stderr, "In repair: Transition back to %s\n", tc.currentStatus)
	case FENCE:
		// it's in a view change.
		log.Info().Msgf("Last operation in the log is fence to update the epoch. We are updating the epoch again.")
	}
	return nil
}

func (tc *TransactionManager) InitTransaction(ctx context.Context) error {
	// Steps:
	// fence first, to stop the zoombie instance from writing to the transaction log
	// clean up the log
	tc.currentStatus = FENCE
	recentTxnMeta, err := tc.getMostRecentTransactionState(ctx)
	if err != nil {
		return fmt.Errorf("getMostRecentTransactionState failed: %v", err)
	}
	// fmt.Fprintf(os.Stderr, "Init transaction: Transition to %s\n", tc.currentStatus)
	if recentTxnMeta == nil {
		tc.genAppId()
		tc.CurrentEpoch = 0
	} else {
		tc.CurrentEpoch = recentTxnMeta.TaskEpoch
		tc.CurrentTaskId = recentTxnMeta.TaskId
	}
	if recentTxnMeta != nil && recentTxnMeta.TaskEpoch == math.MaxUint16 {
		tc.genAppId()
		tc.CurrentEpoch = 0
	}
	tc.CurrentEpoch += 1
	txnMeta := TxnMetadata{
		TaskId:    tc.CurrentTaskId,
		TaskEpoch: tc.CurrentEpoch,
		State:     tc.currentStatus,
	}
	tags := []uint64{NameHashWithPartition(tc.transactionLog.topicNameHash, 0), FenceTag(tc.transactionLog.topicNameHash, 0)}
	err = tc.appendToTransactionLog(ctx, &txnMeta, tags)
	if err != nil {
		return fmt.Errorf("appendToTransactionLog failed: %v", err)
	}

	if recentTxnMeta != nil {
		err = tc.loadAndFixTransaction(ctx, recentTxnMeta)
		if err != nil {
			return fmt.Errorf("loadTransactinoFromLog failed: %v", err)
		}
	}

	return nil
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
				if txnMeta.TaskId == tc.CurrentTaskId && txnMeta.TaskEpoch > tc.CurrentEpoch {
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
	g, ectx := errgroup.WithContext(ctx)
	for topic, partitions := range tc.currentTopicPartition {
		stream := tc.topicStreams[topic]
		for par := range partitions {
			parNum := par
			g.Go(func() error {
				_, err = stream.Push(ectx, msg_encoded, parNum, true)
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
		TaskId:          tc.CurrentTaskId,
		TaskEpoch:       tc.CurrentEpoch,
	}
	err := tc.appendToTransactionLog(ctx, &txnMd, nil)
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
			TaskId:          tc.CurrentTaskId,
			TaskEpoch:       tc.CurrentEpoch,
		}
		return tc.appendToTransactionLog(ctx, &txnMd, nil)
	}
	return nil
}

func (tc *TransactionManager) CreateOffsetTopic(topicToTrack string, numPartition uint8) error {
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
	_, ok := tc.topicStreams[topicToTrack]
	if ok {
		return
	}
	tc.topicStreams[topicToTrack] = stream
}

func (tc *TransactionManager) AddTopicTrackConsumedSeqs(ctx context.Context, topicToTrack string, partitions []uint8) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	return tc.AddTopicPartition(ctx, offsetTopic, partitions)
}

type ConsumedSeqNumConfig struct {
	TopicToTrack   string
	TaskId         uint64
	ConsumedSeqNum uint64
	TaskEpoch      uint16
	Partition      uint8
}

func (tc *TransactionManager) AppendConsumedSeqNum(ctx context.Context, consumedSeqNumConfig ConsumedSeqNumConfig) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + consumedSeqNumConfig.TopicToTrack
	offsetLog := tc.topicStreams[offsetTopic]
	offsetRecord := OffsetRecord{
		Offset:    consumedSeqNumConfig.ConsumedSeqNum,
		TaskId:    consumedSeqNumConfig.TaskId,
		TaskEpoch: consumedSeqNumConfig.TaskEpoch,
	}
	encoded, err := tc.offsetRecordSerde.Encode(&offsetRecord)
	if err != nil {
		return err
	}

	_, err = offsetLog.Push(ctx, encoded, consumedSeqNumConfig.Partition, false)
	return err
}

func (tc *TransactionManager) FindLastConsumedSeqNum(ctx context.Context, topicToTrack string, parNum uint8) (uint64, error) {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	offsetLog := tc.topicStreams[offsetTopic]

	txnMarkerTag := TxnMarkerTag(offsetLog.TopicNameHash(), parNum)
	var txnMkRawMsg *commtypes.RawMsg = nil
	for {
		_, txnMkRawMsg, err := offsetLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, parNum, txnMarkerTag)
		if err != nil {
			return 0, err
		}
		if !txnMkRawMsg.IsControl {
			continue
		} else {
			break
		}
	}
	if txnMkRawMsg == nil {
		return 0, errors.ErrStreamEmpty
	}

	tag := NameHashWithPartition(offsetLog.TopicNameHash(), parNum)

	_, rawMsg, err := offsetLog.ReadBackwardWithTag(ctx, txnMkRawMsg.LogSeqNum, parNum, tag)
	if err != nil {
		return 0, err
	}
	return rawMsg.LogSeqNum, nil
}

func (tc *TransactionManager) BeginTransaction(ctx context.Context) error {
	if !BEGIN.IsValidPreviousState(tc.currentStatus) {
		// fmt.Fprintf(os.Stderr, "current state is %d\n", tc.currentStatus)
		return errors.ErrInvalidStateTransition
	}
	tc.currentStatus = BEGIN
	// fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)

	txnState := TxnMetadata{
		State:     tc.currentStatus,
		TaskId:    tc.CurrentTaskId,
		TaskEpoch: tc.CurrentEpoch,
	}
	tags := []uint64{NameHashWithPartition(tc.transactionLog.topicNameHash, 0), BeginTag(tc.transactionLog.topicNameHash, 0)}
	return tc.appendToTransactionLog(ctx, &txnState, tags)
}

// second phase of the 2-phase commit protocol
func (tc *TransactionManager) completeTransaction(ctx context.Context, trMark TxnMark, trState TransactionState) error {
	// append txn marker to all topic partitions
	err := tc.appendTxnMarkerToStreams(ctx, trMark, tc.CurrentTaskId, tc.CurrentEpoch)
	if err != nil {
		return err
	}
	// async append complete_commit
	tc.currentStatus = trState
	// fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	txnMd := TxnMetadata{
		State: tc.currentStatus,
	}
	tc.backgroundJobErrg.Go(func() error {
		return tc.appendToTransactionLog(tc.backgroundJobCtx, &txnMd, nil)
	})
	return nil
}

func (tc *TransactionManager) CommitTransaction(ctx context.Context) error {
	if !PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		// fmt.Fprintf(os.Stderr, "Fail to transition from %s to PREPARE_COMMIT\n", tc.currentStatus.String())
		return errors.ErrInvalidStateTransition
	}

	// first phase of the commit
	tc.currentStatus = PREPARE_COMMIT
	// fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
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
	if !PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		return errors.ErrInvalidStateTransition
	}
	tc.currentStatus = PREPARE_ABORT
	// fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
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
	// fmt.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	tc.currentTopicPartition = make(map[string]map[uint8]struct{})
}

func (tc *TransactionManager) Close() error {
	// wait for all background go rountine to finish
	return tc.backgroundJobErrg.Wait()
}
