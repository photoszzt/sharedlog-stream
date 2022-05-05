package transaction

import (
	"context"
	"fmt"
	"math"
	"os"
	"sync"

	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/txn_data"

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
// transaction manager is not goroutine safe, it's assumed to be used by only one stream task and only
// one goroutine could update it
type TransactionManager struct {
	backgroundJobCtx    context.Context
	txnMdSerde          commtypes.Serde
	topicPartitionSerde commtypes.Serde
	txnMarkerSerde      commtypes.Serde
	offsetRecordSerde   commtypes.Serde
	env                 types.Environment
	transactionLog      *sharedlog_stream.SharedLogStream
	topicStreams        map[string]*sharedlog_stream.ShardedSharedLogStream
	backgroundJobErrg   *errgroup.Group

	tpMapMu               sync.Mutex
	currentTopicPartition map[string]map[uint8]struct{}

	TransactionalId string
	CurrentTaskId   uint64
	TransactionID   uint64
	CurrentEpoch    uint16
	serdeFormat     commtypes.SerdeFormat

	currentStatus txn_data.TransactionState
}

func BeginTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(txn_data.PartitionBits+txn_data.LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<txn_data.LogTagReserveBits + txn_data.TransactionLogBegin
}

func FenceTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(txn_data.PartitionBits+txn_data.LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<txn_data.LogTagReserveBits + txn_data.TransactionLogFence
}

func NewTransactionManager(ctx context.Context,
	env types.Environment,
	transactional_id string,
	serdeFormat commtypes.SerdeFormat,
) (*TransactionManager, error) {
	log, err := sharedlog_stream.NewSharedLogStream(env, TRANSACTION_LOG_TOPIC_NAME+"_"+transactional_id, serdeFormat)
	if err != nil {
		return nil, err
	}
	errg, gctx := errgroup.WithContext(ctx)
	tm := &TransactionManager{
		transactionLog:        log,
		TransactionalId:       transactional_id,
		currentStatus:         txn_data.EMPTY,
		currentTopicPartition: make(map[string]map[uint8]struct{}),
		topicStreams:          make(map[string]*sharedlog_stream.ShardedSharedLogStream),
		backgroundJobErrg:     errg,
		backgroundJobCtx:      gctx,
		CurrentEpoch:          0,
		CurrentTaskId:         0,
		serdeFormat:           serdeFormat,
		TransactionID:         0,
		env:                   env,
	}
	err = tm.setupSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	return tm, nil
}

func (tc *TransactionManager) setupSerde(serdeFormat commtypes.SerdeFormat) error {
	if serdeFormat == commtypes.JSON {
		tc.txnMdSerde = txn_data.TxnMetadataJSONSerde{}
		tc.topicPartitionSerde = txn_data.TopicPartitionJSONSerde{}
		tc.txnMarkerSerde = txn_data.TxnMarkerJSONSerde{}
		tc.offsetRecordSerde = txn_data.OffsetRecordJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		tc.txnMdSerde = txn_data.TxnMetadataMsgpSerde{}
		tc.topicPartitionSerde = txn_data.TopicPartitionMsgpSerde{}
		tc.txnMarkerSerde = txn_data.TxnMarkerMsgpSerde{}
		tc.offsetRecordSerde = txn_data.OffsetRecordMsgpSerde{}
	} else {
		return fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return nil
}

func (tc *TransactionManager) genAppId() {
	for tc.CurrentTaskId == 0 {
		tc.CurrentTaskId = tc.env.GenerateUniqueID()
	}
}

func (tc *TransactionManager) loadCurrentTopicPartitions(lastTopicPartitions []txn_data.TopicPartition) {
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

func (tc *TransactionManager) getMostRecentTransactionState(ctx context.Context) (*txn_data.TxnMetadata, error) {
	// debug.Fprintf(os.Stderr, "load transaction log\n")
	mostRecentTxnMetadata := &txn_data.TxnMetadata{
		TopicPartitions: make([]txn_data.TopicPartition, 0),
		TaskId:          0,
		TaskEpoch:       0,
		State:           txn_data.EMPTY,
		TransactionID:   0,
	}

	// find the begin of the last transaction
	for {
		_, rawMsg, err := tc.transactionLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, 0, BeginTag(tc.transactionLog.TopicNameHash(), 0))
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
		val, err := tc.txnMdSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, err
		}
		txnMeta := val.(txn_data.TxnMetadata)
		if txnMeta.State != txn_data.BEGIN {
			continue
		} else {
			// read from the begin of the last transaction
			tc.transactionLog.SetCursor(rawMsg.LogSeqNum, 0)
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
			val, err := tc.txnMdSerde.Decode(msg.Payload)
			if err != nil {
				return nil, err
			}
			txnMeta := val.(txn_data.TxnMetadata)

			mostRecentTxnMetadata.State = txnMeta.State
			mostRecentTxnMetadata.TaskId = txnMeta.TaskId
			mostRecentTxnMetadata.TaskEpoch = txnMeta.TaskEpoch
			mostRecentTxnMetadata.TransactionID = txnMeta.TransactionID

			if txnMeta.TopicPartitions != nil {
				mostRecentTxnMetadata.TopicPartitions = append(mostRecentTxnMetadata.TopicPartitions, txnMeta.TopicPartitions...)
			}
		}
	}
	return mostRecentTxnMetadata, nil
}

// each transaction id corresponds to a separate transaction log; we only have one transaction id per serverless function
func (tc *TransactionManager) loadAndFixTransaction(ctx context.Context, mostRecentTxnMetadata *txn_data.TxnMetadata) error {
	// check the last status of the transaction
	switch mostRecentTxnMetadata.State {
	case txn_data.EMPTY, txn_data.COMPLETE_COMMIT, txn_data.COMPLETE_ABORT:
		log.Info().Msgf("examed previous transaction with no error")
	case txn_data.BEGIN:
		// need to abort
		currentStatus := tc.currentStatus

		// use the previous app id to finish the previous transaction
		tc.currentStatus = mostRecentTxnMetadata.State
		// debug.Fprintf(os.Stderr, "In repair: Transition to %s to restore\n", tc.currentStatus)

		// debug.Fprintf(os.Stderr, "before load current topic partitions\n")
		tc.loadCurrentTopicPartitions(mostRecentTxnMetadata.TopicPartitions)
		// debug.Fprintf(os.Stderr, "after load current topic partitions\n")
		err := tc.AbortTransaction(ctx, true, nil, nil)
		// debug.Fprintf(os.Stderr, "after abort transactions\n")
		if err != nil {
			return err
		}
		// swap back
		tc.currentStatus = currentStatus
		// debug.Fprintf(os.Stderr, "In repair: Transition back to %s\n", tc.currentStatus)
	case txn_data.PREPARE_ABORT:
		// need to abort
		currentStatus := tc.currentStatus

		tc.currentStatus = mostRecentTxnMetadata.State
		// debug.Fprintf(os.Stderr, "In repair: Transition to %s to restore\n", tc.currentStatus)

		// the transaction is aborted but the marker might not pushed to the relevant partitions yet
		tc.loadCurrentTopicPartitions(mostRecentTxnMetadata.TopicPartitions)
		err := tc.completeTransaction(ctx, txn_data.ABORT, txn_data.COMPLETE_ABORT)
		if err != nil {
			return err
		}
		tc.cleanupState()
		// swap back
		tc.currentStatus = currentStatus
		// debug.Fprintf(os.Stderr, "In repair: Transition back to %s\n", tc.currentStatus)
	case txn_data.PREPARE_COMMIT:
		// the transaction is commited but the marker might not pushed to the relevant partitions yet
		// need to commit
		currentStatus := tc.currentStatus

		tc.currentStatus = mostRecentTxnMetadata.State
		// debug.Fprintf(os.Stderr, "In repair: Transition to %s to restore\n", tc.currentStatus)

		tc.loadCurrentTopicPartitions(mostRecentTxnMetadata.TopicPartitions)
		err := tc.completeTransaction(ctx, txn_data.COMMIT, txn_data.COMPLETE_COMMIT)
		if err != nil {
			return err
		}
		tc.cleanupState()

		// swap back
		tc.currentStatus = currentStatus
		// debug.Fprintf(os.Stderr, "In repair: Transition back to %s\n", tc.currentStatus)
	case txn_data.FENCE:
		// it's in a view change.
		log.Info().Msgf("Last operation in the log is fence to update the epoch. We are updating the epoch again.")
	}
	return nil
}

// call at the beginning of function. Expected to execute in a single thread
func (tc *TransactionManager) InitTransaction(ctx context.Context) error {
	// Steps:
	// fence first, to stop the zoombie instance from writing to the transaction log
	// clean up the log
	tc.currentStatus = txn_data.FENCE
	recentTxnMeta, err := tc.getMostRecentTransactionState(ctx)
	if err != nil {
		return fmt.Errorf("getMostRecentTransactionState failed: %v", err)
	}
	// debug.Fprintf(os.Stderr, "Init transaction: Transition to %s\n", tc.currentStatus)
	if recentTxnMeta == nil {
		tc.genAppId()
		tc.CurrentEpoch = 0
	} else {
		tc.CurrentEpoch = recentTxnMeta.TaskEpoch
		tc.CurrentTaskId = recentTxnMeta.TaskId
		tc.TransactionID = recentTxnMeta.TransactionID
	}
	if recentTxnMeta != nil && recentTxnMeta.TaskEpoch == math.MaxUint16 {
		tc.genAppId()
		tc.CurrentEpoch = 0
	}
	tc.CurrentEpoch += 1
	txnMeta := txn_data.TxnMetadata{
		TaskId:    tc.CurrentTaskId,
		TaskEpoch: tc.CurrentEpoch,
		State:     tc.currentStatus,
	}
	tags := []uint64{sharedlog_stream.NameHashWithPartition(tc.transactionLog.TopicNameHash(), 0), FenceTag(tc.transactionLog.TopicNameHash(), 0)}
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

// monitoring entry with fence tag
func (tc *TransactionManager) MonitorTransactionLog(ctx context.Context, quit chan struct{},
	errc chan error, dcancel context.CancelFunc,
) {
	fenceTag := FenceTag(tc.transactionLog.TopicNameHash(), 0)
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
			return
		} else {
			for _, rawMsg := range rawMsgs {
				txnMetaTmp, err := tc.txnMdSerde.Decode(rawMsg.Payload)
				if err != nil {
					errc <- err
					return
				}
				txnMeta := txnMetaTmp.(txn_data.TxnMetadata)
				if txnMeta.State != txn_data.FENCE {
					panic("state should be fence")
				}
				if txnMeta.TaskId == tc.CurrentTaskId && txnMeta.TaskEpoch > tc.CurrentEpoch {
					// I'm the zoombie
					dcancel()
					errc <- nil
					return
				}
			}
		}
	}
}

func (tc *TransactionManager) appendToTransactionLog(ctx context.Context, tm *txn_data.TxnMetadata, tags []uint64) error {
	encoded, err := tc.txnMdSerde.Encode(tm)
	if err != nil {
		return fmt.Errorf("txnMdSerde enc err: %v", tm)
	}
	if tags != nil {
		_, err = tc.transactionLog.PushWithTag(ctx, encoded, 0, tags, false, false)
	} else {
		_, err = tc.transactionLog.Push(ctx, encoded, 0, false, false)
	}
	return err
}

func (tc *TransactionManager) appendTxnMarkerToStreams(ctx context.Context, marker txn_data.TxnMark) error {
	tm := txn_data.TxnMarker{
		Mark:               uint8(marker),
		TranIDOrScaleEpoch: tc.TransactionID,
	}
	encoded, err := tc.txnMarkerSerde.Encode(&tm)
	if err != nil {
		return err
	}
	g, ectx := errgroup.WithContext(ctx)
	for topic, partitions := range tc.currentTopicPartition {
		stream := tc.topicStreams[topic]
		err := stream.Flush(ctx)
		if err != nil {
			return err
		}
		for par := range partitions {
			parNum := par
			g.Go(func() error {
				off, err := stream.Push(ectx, encoded, parNum, true, false)
				debug.Fprintf(os.Stderr, "append marker %d to stream %s off %x\n", marker, stream.TopicName(), off)
				return err
			})
		}
	}
	return g.Wait()
}

func (tc *TransactionManager) registerTopicPartitions(ctx context.Context) error {
	var tps []txn_data.TopicPartition
	for topic, pars := range tc.currentTopicPartition {
		pars_arr := make([]uint8, 0, len(pars))
		for p := range pars {
			pars_arr = append(pars_arr, p)
		}
		tp := txn_data.TopicPartition{
			Topic:  topic,
			ParNum: pars_arr,
		}
		tps = append(tps, tp)
	}
	txnMd := txn_data.TxnMetadata{
		TopicPartitions: tps,
		State:           tc.currentStatus,
		// TaskId:          tc.CurrentTaskId,
		// TaskEpoch:       tc.CurrentEpoch,
	}
	err := tc.appendToTransactionLog(ctx, &txnMd, nil)
	return err
}

func (tc *TransactionManager) checkTopicExistsInTopicStream(topic string) bool {
	_, ok := tc.topicStreams[topic]
	return ok
}

// this function could be called by multiple goroutine.
func (tc *TransactionManager) AddTopicPartition(ctx context.Context, topic string, partitions []uint8) error {
	if tc.currentStatus != txn_data.BEGIN {
		return xerrors.Errorf("should begin transaction first")
	}
	debug.Assert(tc.checkTopicExistsInTopicStream(topic), fmt.Sprintf("topic %s's stream should be tracked", topic))
	// debug.Fprintf(os.Stderr, "tracking topic %s par %v\n", topic, partitions)
	needToAppendToLog := false
	tc.tpMapMu.Lock()
	defer tc.tpMapMu.Unlock()
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
		txnMd := txn_data.TxnMetadata{
			TopicPartitions: []txn_data.TopicPartition{{Topic: topic, ParNum: partitions}},
			State:           tc.currentStatus,
			// TaskId:          tc.CurrentTaskId,
			// TaskEpoch:       tc.CurrentEpoch,
		}
		return tc.appendToTransactionLog(ctx, &txnMd, nil)
	}
	return nil
}

func (tc *TransactionManager) createOffsetTopic(topicToTrack string, numPartition uint8) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	_, ok := tc.topicStreams[offsetTopic]
	if ok {
		// already exists
		return nil
	}
	off, err := sharedlog_stream.NewShardedSharedLogStream(tc.env, offsetTopic, numPartition, tc.serdeFormat)
	if err != nil {
		return err
	}
	tc.topicStreams[offsetTopic] = off
	off.SetTaskId(tc.CurrentTaskId)
	off.SetTaskEpoch(tc.CurrentEpoch)
	return nil
}

func (tc *TransactionManager) RecordTopicStreams(topicToTrack string, stream *sharedlog_stream.ShardedSharedLogStream) {
	_, ok := tc.topicStreams[topicToTrack]
	if ok {
		return
	}
	tc.topicStreams[topicToTrack] = stream
	debug.Fprintf(os.Stderr, "tracking stream %s\n", topicToTrack)
	stream.SetTaskId(tc.CurrentTaskId)
	stream.SetTaskEpoch(tc.CurrentEpoch)
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

func OffsetTopic(topicToTrack string) string {
	return CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
}

type ConsumeSeqManager struct {
	offsetRecordSerde commtypes.Serde
	txnMarkerSerde    commtypes.Serde
	mapMu             sync.Mutex
	curConsumePar     map[string]map[uint8]struct{}
	offsetLogs        map[string]*sharedlog_stream.ShardedSharedLogStream
}

func (cm *ConsumeSeqManager) AddTopicTrackConsumedSeqs(ctx context.Context, topicToTrack string, partitions []uint8) {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	cm.TrackTopicPartition(offsetTopic, partitions)
}

func (cm *ConsumeSeqManager) TrackTopicPartition(topic string, partitions []uint8) {
	cm.mapMu.Lock()
	defer cm.mapMu.Unlock()
	parSet, ok := cm.curConsumePar[topic]
	if !ok {
		parSet = make(map[uint8]struct{})
	}
	for _, parNum := range partitions {
		_, ok = parSet[parNum]
		if !ok {
			parSet[parNum] = struct{}{}
		}
	}
	cm.curConsumePar[topic] = parSet
}

func (cm *ConsumeSeqManager) CreateOffsetTopic(env types.Environment, topicToTrack string,
	numPartition uint8, serdeFormat commtypes.SerdeFormat,
) error {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	_, ok := cm.offsetLogs[offsetTopic]
	if ok {
		// already exists
		return nil
	}
	off, err := sharedlog_stream.NewShardedSharedLogStream(env, offsetTopic, numPartition, serdeFormat)
	if err != nil {
		return err
	}
	cm.offsetLogs[offsetTopic] = off
	return nil
}

func NewConsumeSeqManager(serdeFormat commtypes.SerdeFormat) (*ConsumeSeqManager, error) {
	var offsetRecordSerde commtypes.Serde
	var txnMarkerSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		offsetRecordSerde = txn_data.OffsetRecordJSONSerde{}
		txnMarkerSerde = txn_data.TxnMarkerJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		offsetRecordSerde = txn_data.OffsetRecordMsgpSerde{}
		txnMarkerSerde = txn_data.TxnMarkerMsgpSerde{}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return &ConsumeSeqManager{
		txnMarkerSerde:    txnMarkerSerde,
		offsetRecordSerde: offsetRecordSerde,
		offsetLogs:        make(map[string]*sharedlog_stream.ShardedSharedLogStream),
		curConsumePar:     make(map[string]map[uint8]struct{}),
	}, nil
}

func (cm *ConsumeSeqManager) AppendConsumedSeqNum(ctx context.Context, consumedSeqNumConfigs []ConsumedSeqNumConfig) error {
	for _, consumedSeqNumConfig := range consumedSeqNumConfigs {
		offsetTopic := OffsetTopic(consumedSeqNumConfig.TopicToTrack)
		offsetLog := cm.offsetLogs[offsetTopic]
		offsetRecord := txn_data.OffsetRecord{
			Offset: consumedSeqNumConfig.ConsumedSeqNum,
		}
		encoded, err := cm.offsetRecordSerde.Encode(&offsetRecord)
		if err != nil {
			return err
		}

		_, err = offsetLog.Push(ctx, encoded, consumedSeqNumConfig.Partition, false, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (cm *ConsumeSeqManager) FindLastConsumedSeqNum(ctx context.Context, topicToTrack string, parNum uint8) (uint64, error) {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	offsetLog := cm.offsetLogs[offsetTopic]
	debug.Assert(offsetTopic == offsetLog.TopicName(), fmt.Sprintf("expected offset log tp: %s, got %s",
		offsetTopic, offsetLog.TopicName()))
	// debug.Fprintf(os.Stderr, "looking at offsetlog %s, offsetLog tp: %s\n", offsetTopic, offsetLog.TopicName())

	// find the most recent transaction marker
	txnMarkerTag := sharedlog_stream.TxnMarkerTag(offsetLog.TopicNameHash(), parNum)
	var txnMkRawMsg *commtypes.RawMsg = nil
	var err error
	for {
		_, txnMkRawMsg, err = offsetLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, parNum, txnMarkerTag)
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

	tag := sharedlog_stream.NameHashWithPartition(offsetLog.TopicNameHash(), parNum)

	// read the previous item which should record the offset number
	_, rawMsg, err := offsetLog.ReadBackwardWithTag(ctx, txnMkRawMsg.LogSeqNum, parNum, tag)
	if err != nil {
		return 0, err
	}
	return rawMsg.LogSeqNum, nil
}

func (cm *ConsumeSeqManager) Commit(ctx context.Context) error {
	tm := txn_data.TxnMarker{
		Mark: uint8(txn_data.COMMIT),
	}
	encoded, err := cm.txnMarkerSerde.Encode(&tm)
	if err != nil {
		return err
	}
	g, ectx := errgroup.WithContext(ctx)
	for topic, partitions := range cm.curConsumePar {
		stream := cm.offsetLogs[topic]
		err := stream.Flush(ctx)
		if err != nil {
			return err
		}
		for par := range partitions {
			parNum := par
			g.Go(func() error {
				off, err := stream.Push(ectx, encoded, parNum, true, false)
				debug.Fprintf(os.Stderr, "append marker %d to stream %s off %x\n", txn_data.COMMIT, stream.TopicName(), off)
				return err
			})
		}
	}
	err = g.Wait()
	if err != nil {
		return err
	}
	cm.curConsumePar = make(map[string]map[uint8]struct{})
	return nil
}

func (tc *TransactionManager) AppendConsumedSeqNum(ctx context.Context, consumedSeqNumConfigs []ConsumedSeqNumConfig) error {
	for _, consumedSeqNumConfig := range consumedSeqNumConfigs {
		offsetTopic := OffsetTopic(consumedSeqNumConfig.TopicToTrack)
		offsetLog := tc.topicStreams[offsetTopic]
		offsetRecord := txn_data.OffsetRecord{
			Offset:    consumedSeqNumConfig.ConsumedSeqNum,
			TaskId:    consumedSeqNumConfig.TaskId,
			TaskEpoch: consumedSeqNumConfig.TaskEpoch,
		}
		encoded, err := tc.offsetRecordSerde.Encode(&offsetRecord)
		if err != nil {
			return err
		}

		_, err = offsetLog.Push(ctx, encoded, consumedSeqNumConfig.Partition, false, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tc *TransactionManager) FindLastConsumedSeqNum(ctx context.Context, topicToTrack string, parNum uint8) (uint64, error) {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	offsetLog := tc.topicStreams[offsetTopic]
	debug.Assert(offsetTopic == offsetLog.TopicName(), fmt.Sprintf("expected offset log tp: %s, got %s",
		offsetTopic, offsetLog.TopicName()))
	// debug.Fprintf(os.Stderr, "looking at offsetlog %s, offsetLog tp: %s\n", offsetTopic, offsetLog.TopicName())

	// find the most recent transaction marker
	txnMarkerTag := sharedlog_stream.TxnMarkerTag(offsetLog.TopicNameHash(), parNum)
	var txnMkRawMsg *commtypes.RawMsg = nil
	var err error
	for {
		_, txnMkRawMsg, err = offsetLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, parNum, txnMarkerTag)
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

	tag := sharedlog_stream.NameHashWithPartition(offsetLog.TopicNameHash(), parNum)

	// read the previous item which should record the offset number
	_, rawMsg, err := offsetLog.ReadBackwardWithTag(ctx, txnMkRawMsg.LogSeqNum, parNum, tag)
	if err != nil {
		return 0, err
	}
	return rawMsg.LogSeqNum, nil
}

func (tc *TransactionManager) FindConsumedSeqNumMatchesTransactionID(ctx context.Context, topicToTrack string, parNum uint8,
	transactionID uint64,
) (uint64, error) {
	offsetTopic := CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	offsetLog := tc.topicStreams[offsetTopic]
	debug.Assert(offsetTopic == offsetLog.TopicName(), fmt.Sprintf("expected offset log tp: %s, got %s",
		offsetTopic, offsetLog.TopicName()))
	txnMarkerTag := sharedlog_stream.TxnMarkerTag(offsetLog.TopicNameHash(), parNum)
	var txnMkRawMsg *commtypes.RawMsg = nil
	tailSeqNum := protocol.MaxLogSeqnum
	var err error
	for {
		_, txnMkRawMsg, err = offsetLog.ReadBackwardWithTag(ctx, tailSeqNum, parNum, txnMarkerTag)
		if err != nil {
			return 0, err
		}
		if !txnMkRawMsg.IsControl {
			continue
		} else {
			txnMarkerTmp, err := tc.txnMarkerSerde.Decode(txnMkRawMsg.Payload)
			if err != nil {
				return 0, err
			}
			txnMarker := txnMarkerTmp.(txn_data.TxnMarker)
			if txnMarker.TranIDOrScaleEpoch == transactionID {
				break
			}
		}
	}
	if txnMkRawMsg == nil {
		return 0, errors.ErrStreamEmpty
	}
	tag := sharedlog_stream.NameHashWithPartition(offsetLog.TopicNameHash(), parNum)

	// read the previous item which should record the offset number
	_, rawMsg, err := offsetLog.ReadBackwardWithTag(ctx, txnMkRawMsg.LogSeqNum, parNum, tag)
	if err != nil {
		return 0, err
	}
	return rawMsg.LogSeqNum, nil
}

func (tc *TransactionManager) BeginTransaction(ctx context.Context, kvstores []*KVStoreChangelog,
	winstores []*WindowStoreChangelog,
) error {
	if !txn_data.BEGIN.IsValidPreviousState(tc.currentStatus) {
		debug.Fprintf(os.Stderr, "fail to transition from %v to BEGIN\n", tc.currentStatus)
		return errors.ErrInvalidStateTransition
	}
	tc.currentStatus = txn_data.BEGIN
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)

	tc.TransactionID += 1
	txnState := txn_data.TxnMetadata{
		State:         tc.currentStatus,
		TaskId:        tc.CurrentTaskId,
		TaskEpoch:     tc.CurrentEpoch,
		TransactionID: tc.TransactionID,
	}
	tags := []uint64{sharedlog_stream.NameHashWithPartition(tc.transactionLog.TopicNameHash(), 0), BeginTag(tc.transactionLog.TopicNameHash(), 0)}
	if err := tc.appendToTransactionLog(ctx, &txnState, tags); err != nil {
		return err
	}
	if err := BeginKVStoreTransaction(ctx, kvstores); err != nil {
		return err
	}
	return BeginWindowStoreTransaction(ctx, winstores)
}

// second phase of the 2-phase commit protocol
func (tc *TransactionManager) completeTransaction(ctx context.Context, trMark txn_data.TxnMark, trState txn_data.TransactionState) error {
	// append txn marker to all topic partitions
	err := tc.appendTxnMarkerToStreams(ctx, trMark)
	if err != nil {
		return err
	}
	// async append complete_commit
	tc.currentStatus = trState
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	txnMd := txn_data.TxnMetadata{
		State: tc.currentStatus,
	}
	tc.backgroundJobErrg.Go(func() error {
		return tc.appendToTransactionLog(tc.backgroundJobCtx, &txnMd, nil)
	})
	return nil
}

func (tc *TransactionManager) CommitTransaction(ctx context.Context, kvstores []*KVStoreChangelog,
	winstores []*WindowStoreChangelog,
) error {
	if !txn_data.PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		debug.Fprintf(os.Stderr, "Fail to transition from %s to PREPARE_COMMIT\n", tc.currentStatus.String())
		return errors.ErrInvalidStateTransition
	}

	// first phase of the commit
	tc.currentStatus = txn_data.PREPARE_COMMIT
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	err := tc.registerTopicPartitions(ctx)
	if err != nil {
		return err
	}
	if err := CommitKVStoreTransaction(ctx, kvstores, tc.TransactionID); err != nil {
		return err
	}
	if err := CommitWindowStoreTransaction(ctx, winstores, tc.TransactionID); err != nil {
		return err
	}
	// second phase of the commit
	err = tc.completeTransaction(ctx, txn_data.COMMIT, txn_data.COMPLETE_COMMIT)
	if err != nil {
		return err
	}

	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) AbortTransaction(ctx context.Context, inRestore bool,
	kvstores []*KVStoreChangelog,
	winstores []*WindowStoreChangelog,
) error {
	if !txn_data.PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		debug.Fprintf(os.Stderr, "fail to transition state from %d to PRE_ABORT", tc.currentStatus)
		return errors.ErrInvalidStateTransition
	}
	tc.currentStatus = txn_data.PREPARE_ABORT
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	err := tc.registerTopicPartitions(ctx)
	if err != nil {
		return err
	}

	if !inRestore {
		if err := AbortKVStoreTransaction(ctx, kvstores); err != nil {
			return err
		}
		if err := AbortWindowStoreTransaction(ctx, winstores); err != nil {
			return err
		}
	}

	err = tc.completeTransaction(ctx, txn_data.ABORT, txn_data.COMPLETE_ABORT)
	if err != nil {
		return err
	}
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) cleanupState() {
	tc.currentStatus = txn_data.EMPTY
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	tc.currentTopicPartition = make(map[string]map[uint8]struct{})
}

func (tc *TransactionManager) Close() error {
	// wait for all background go rountine to finish
	return tc.backgroundJobErrg.Wait()
}
