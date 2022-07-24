package transaction

import (
	"context"
	"fmt"
	"math"
	"os"

	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/consume_seq_num_manager/con_types"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils/syncutils"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/sync/errgroup"
)

const (
	TRANSACTION_LOG_TOPIC_NAME = "__transaction_log"
)

// assume each transactional_id correspond to one output partition
// transaction manager is not goroutine safe, it's assumed to be used by
// only one stream task and only one goroutine could update it
type TransactionManager struct {
	tpMapMu               syncutils.Mutex
	currentTopicSubstream map[string]map[uint8]struct{}

	backgroundJobCtx    context.Context
	txnMdSerde          commtypes.SerdeG[txn_data.TxnMetadata]
	topicPartitionSerde commtypes.SerdeG[txn_data.TopicPartition]
	txnMarkerSerde      commtypes.SerdeG[commtypes.EpochMarker]
	offsetRecordSerde   commtypes.SerdeG[txn_data.OffsetRecord]
	env                 types.Environment
	transactionLog      *sharedlog_stream.SharedLogStream
	topicStreams        map[string]*sharedlog_stream.ShardedSharedLogStream
	backgroundJobErrg   *errgroup.Group

	TransactionalId string
	transactionID   uint64
	commtypes.ProducerId
	serdeFormat commtypes.SerdeFormat

	currentStatus txn_data.TransactionState
	errChan       chan error
	quitChan      chan struct{}
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
		currentTopicSubstream: make(map[string]map[uint8]struct{}),
		topicStreams:          make(map[string]*sharedlog_stream.ShardedSharedLogStream),
		backgroundJobErrg:     errg,
		backgroundJobCtx:      gctx,
		ProducerId:            commtypes.NewProducerId(),
		serdeFormat:           serdeFormat,
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
		tc.txnMdSerde = txn_data.TxnMetadataJSONSerdeG{}
		tc.topicPartitionSerde = txn_data.TopicPartitionJSONSerdeG{}
		tc.txnMarkerSerde = commtypes.EpochMarkerJSONSerdeG{}
		tc.offsetRecordSerde = txn_data.OffsetRecordJSONSerdeG{}
	} else if serdeFormat == commtypes.MSGP {
		tc.txnMdSerde = txn_data.TxnMetadataMsgpSerdeG{}
		tc.topicPartitionSerde = txn_data.TopicPartitionMsgpSerdeG{}
		tc.txnMarkerSerde = commtypes.EpochMarkerMsgpSerdeG{}
		tc.offsetRecordSerde = txn_data.OffsetRecordMsgpSerdeG{}
	} else {
		return fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
	return nil
}

func (tc *TransactionManager) loadCurrentTopicPartitions(lastTopicPartitions []txn_data.TopicPartition) {
	for _, tp := range lastTopicPartitions {
		pars, ok := tc.currentTopicSubstream[tp.Topic]
		if !ok {
			pars = make(map[uint8]struct{})
		}
		for _, par := range tp.ParNum {
			pars[par] = struct{}{}
		}
	}
}

func (tc *TransactionManager) getMostRecentTransactionState(ctx context.Context) (*txn_data.TxnMetadata, commtypes.ProducerId, error) {
	// debug.Fprintf(os.Stderr, "load transaction log\n")
	mostRecentTxnMetadata := &txn_data.TxnMetadata{
		TopicPartitions: make([]txn_data.TopicPartition, 0),
		State:           txn_data.EMPTY,
	}
	recentProdId := commtypes.ProducerId{}

	// find the begin of the last transaction
	for {
		rawMsg, err := tc.transactionLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, 0,
			txn_data.BeginTag(tc.transactionLog.TopicNameHash(), 0))
		if err != nil {
			// empty log
			if common_errors.IsStreamEmptyError(err) {
				return nil, commtypes.EmptyProducerId, nil
			}
			return nil, commtypes.EmptyProducerId, err
		}
		txnMeta, err := tc.txnMdSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, commtypes.EmptyProducerId, err
		}
		if txnMeta.State != txn_data.BEGIN {
			continue
		} else {
			// read from the begin of the last transaction
			tc.transactionLog.SetCursor(rawMsg.LogSeqNum, 0)
			recentProdId.TransactionID = rawMsg.LogSeqNum
			break
		}
	}

	for {
		msg, err := tc.transactionLog.ReadNext(ctx, 0)
		if common_errors.IsStreamEmptyError(err) {
			break
		} else if err != nil {
			return nil, commtypes.EmptyProducerId, err
		}
		txnMeta, err := tc.txnMdSerde.Decode(msg.Payload)
		if err != nil {
			return nil, commtypes.EmptyProducerId, err
		}

		if txnMeta.TopicPartitions != nil {
			mostRecentTxnMetadata.TopicPartitions = append(mostRecentTxnMetadata.TopicPartitions, txnMeta.TopicPartitions...)
			mostRecentTxnMetadata.State = txnMeta.State
			recentProdId = msg.ProdId
		} else {
			mostRecentTxnMetadata.State = txnMeta.State
			recentProdId = msg.ProdId
		}
	}
	return mostRecentTxnMetadata, recentProdId, nil
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
		err := tc.AbortTransaction(ctx)
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
		err := tc.completeTransaction(ctx, commtypes.ABORT, txn_data.COMPLETE_ABORT)
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
		err := tc.completeTransaction(ctx, commtypes.EPOCH_END, txn_data.COMPLETE_COMMIT)
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
	recentTxnMeta, recentProdID, err := tc.getMostRecentTransactionState(ctx)
	if err != nil {
		return fmt.Errorf("getMostRecentTransactionState failed: %v", err)
	}
	// debug.Fprintf(os.Stderr, "Init transaction: Transition to %s\n", tc.currentStatus)
	if recentTxnMeta == nil {
		tc.InitTaskId(tc.env)
		tc.TaskEpoch = 0
	} else {
		tc.TaskEpoch = recentProdID.TaskEpoch
		tc.TaskId = recentProdID.TaskId
		tc.transactionID = recentProdID.TransactionID
	}
	if recentTxnMeta != nil && recentProdID.TaskEpoch == math.MaxUint16 {
		tc.InitTaskId(tc.env)
		tc.TaskEpoch = 0
	}
	tc.TaskEpoch += 1
	txnMeta := txn_data.TxnMetadata{
		State: tc.currentStatus,
	}
	tags := []uint64{
		sharedlog_stream.NameHashWithPartition(tc.transactionLog.TopicNameHash(), 0),
		txn_data.FenceTag(tc.transactionLog.TopicNameHash(), 0)}
	_, err = tc.appendToTransactionLog(ctx, txnMeta, tags)
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
func (tc *TransactionManager) monitorTransactionLog(ctx context.Context,
	quit chan struct{}, errc chan error, dcancel context.CancelFunc,
) {
	fenceTag := txn_data.FenceTag(tc.transactionLog.TopicNameHash(), 0)
	for {
		select {
		case <-quit:
			return
		default:
		}
		rawMsg, err := tc.transactionLog.ReadNextWithTag(ctx, 0, fenceTag)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				continue
			}
			errc <- err
			return
		} else {
			txnMeta, err := tc.txnMdSerde.Decode(rawMsg.Payload)
			if err != nil {
				errc <- err
				return
			}
			if txnMeta.State != txn_data.FENCE {
				panic("state should be fence")
			}
			if rawMsg.ProdId.TaskId == tc.GetCurrentTaskId() && rawMsg.ProdId.TaskEpoch > tc.GetCurrentEpoch() {
				// I'm the zoombie
				dcancel()
				errc <- nil
				return
			}
		}
	}
}

func (tc *TransactionManager) StartMonitorLog(ctx context.Context,
	dcancel context.CancelFunc,
) {
	tc.quitChan = make(chan struct{})
	tc.errChan = make(chan error, 1)
	go tc.monitorTransactionLog(ctx, tc.quitChan, tc.errChan, dcancel)
}

func (tc *TransactionManager) SendQuit() {
	tc.quitChan <- struct{}{}
}

func (tc *TransactionManager) ErrChan() chan error {
	return tc.errChan
}

func (tc *TransactionManager) appendToTransactionLog(ctx context.Context,
	tm txn_data.TxnMetadata, tags []uint64,
) (uint64, error) {
	encoded, err := tc.txnMdSerde.Encode(tm)
	if err != nil {
		return 0, fmt.Errorf("txnMdSerde enc err: %v", tm)
	}
	prodId := tc.GetProducerId()
	if tags != nil {
		return tc.transactionLog.PushWithTag(ctx, encoded, 0, tags, nil,
			sharedlog_stream.SingleDataRecordMeta, prodId)
	} else {
		return tc.transactionLog.Push(ctx, encoded, 0,
			sharedlog_stream.SingleDataRecordMeta, prodId)
	}
}

func (tc *TransactionManager) appendTxnMarkerToStreams(ctx context.Context, marker commtypes.EpochMark) error {
	tm := commtypes.EpochMarker{
		Mark: marker,
	}
	encoded, err := tc.txnMarkerSerde.Encode(tm)
	if err != nil {
		return err
	}
	g, ectx := errgroup.WithContext(ctx)
	producerId := tc.GetProducerId()
	for topic, partitions := range tc.currentTopicSubstream {
		stream := tc.topicStreams[topic]
		err := stream.Flush(ctx, producerId)
		if err != nil {
			return err
		}
		for par := range partitions {
			parNum := par
			g.Go(func() error {
				tag := txn_data.MarkerTag(stream.TopicNameHash(), parNum)
				tag2 := sharedlog_stream.NameHashWithPartition(stream.TopicNameHash(), parNum)
				off, err := stream.PushWithTag(ectx, encoded, parNum, []uint64{tag, tag2},
					nil, sharedlog_stream.ControlRecordMeta, producerId)
				debug.Fprintf(os.Stderr, "append marker %d to stream %s off %x tag %x\n",
					marker, stream.TopicName(), off, tag)
				return err
			})
		}
	}
	return g.Wait()
}

func (tc *TransactionManager) append_pre_state(ctx context.Context) error {
	var tps []txn_data.TopicPartition
	for topic, pars := range tc.currentTopicSubstream {
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
	}
	_, err := tc.appendToTransactionLog(ctx, txnMd, nil)
	return err
}

func (tc *TransactionManager) checkTopicExistsInTopicStream(topic string) bool {
	_, ok := tc.topicStreams[topic]
	return ok
}

// this function could be called by multiple goroutine.
func (tc *TransactionManager) AddTopicSubstream(ctx context.Context, topic string, subStreamNum uint8) error {
	if tc.currentStatus != txn_data.BEGIN {
		panic("should begin transaction first")
	}
	debug.Assert(tc.checkTopicExistsInTopicStream(topic), fmt.Sprintf("topic %s's stream should be tracked", topic))
	// debug.Fprintf(os.Stderr, "tracking topic %s par %v\n", topic, partitions)
	needToAppendToLog := false
	tc.tpMapMu.Lock()
	defer tc.tpMapMu.Unlock()
	parSet, ok := tc.currentTopicSubstream[topic]
	if !ok {
		parSet = make(map[uint8]struct{})
		needToAppendToLog = true
	}
	_, ok = parSet[subStreamNum]
	if !ok {
		needToAppendToLog = true
		parSet[subStreamNum] = struct{}{}
	}
	tc.currentTopicSubstream[topic] = parSet
	if needToAppendToLog {
		txnMd := txn_data.TxnMetadata{
			TopicPartitions: []txn_data.TopicPartition{{Topic: topic, ParNum: []uint8{subStreamNum}}},
			State:           tc.currentStatus,
		}
		_, err := tc.appendToTransactionLog(ctx, txnMd, nil)
		return err
	}
	return nil
}

func (tc *TransactionManager) createOffsetTopic(topicToTrack string, numPartition uint8) error {
	offsetTopic := con_types.OffsetTopic(topicToTrack)
	debug.Assert(tc.topicStreams != nil, "topic streams should be initialized")
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
	return nil
}

func (tc *TransactionManager) RecordTopicStreams(topicToTrack string, stream *sharedlog_stream.ShardedSharedLogStream) {
	tc.topicStreams[topicToTrack] = stream
	debug.Fprintf(os.Stderr, "tracking stream %s, stream ptr %v\n", topicToTrack, stream)
}

func (tc *TransactionManager) AddTopicTrackConsumedSeqs(ctx context.Context, topicToTrack string, partition uint8) error {
	offsetTopic := con_types.OffsetTopic(topicToTrack)
	return tc.AddTopicSubstream(ctx, offsetTopic, partition)
}

// finding the last commited marker and gets the marker's seq number
// used in restore and in one thread
func CreateOffsetTopicAndGetOffset(ctx context.Context, tm *TransactionManager,
	topic string, numPartition uint8, parNum uint8,
) (uint64, error) {
	err := tm.createOffsetTopic(topic, numPartition)
	if err != nil {
		return 0, fmt.Errorf("create offset topic failed: %v", err)
	}
	debug.Fprintf(os.Stderr, "created offset topic\n")
	offset, err := tm.FindLastConsumedSeqNum(ctx, topic, parNum)
	if err != nil {
		if !common_errors.IsStreamEmptyError(err) {
			return 0, err
		}
	}
	return offset, nil
}

func CollectOffsetRecords(consumers []producer_consumer.MeteredConsumerIntr) map[string]txn_data.OffsetRecord {
	ret := make(map[string]txn_data.OffsetRecord)
	for _, consumer := range consumers {
		topic := consumer.TopicName()
		offset := consumer.CurrentConsumedSeqNum()
		offsetTopic := con_types.OffsetTopic(topic)
		offsetRecord := txn_data.OffsetRecord{
			Offset: offset,
		}
		ret[offsetTopic] = offsetRecord
	}
	return ret
}

func (tc *TransactionManager) AppendConsumedSeqNum(ctx context.Context, encodedOffsetRecord map[string]txn_data.OffsetRecord, parNum uint8) error {
	for offsetTopic, offsetRecord := range encodedOffsetRecord {
		offsetLog := tc.topicStreams[offsetTopic]

		encoded, err := tc.offsetRecordSerde.Encode(offsetRecord)
		if err != nil {
			return err
		}
		_, err = offsetLog.Push(ctx, encoded, parNum, sharedlog_stream.SingleDataRecordMeta,
			tc.GetProducerId())
		if err != nil {
			return err
		}
		debug.Fprintf(os.Stderr, "consumed offset 0x%x for %s\n", offsetRecord.Offset, offsetTopic)
	}
	return nil
}

func (tc *TransactionManager) FindLastConsumedSeqNum(ctx context.Context, topicToTrack string, parNum uint8) (uint64, error) {
	offsetTopic := con_types.OffsetTopic(topicToTrack)
	offsetLog := tc.topicStreams[offsetTopic]
	debug.Assert(offsetTopic == offsetLog.TopicName(), fmt.Sprintf("expected offset log tp: %s, got %s",
		offsetTopic, offsetLog.TopicName()))
	// debug.Fprintf(os.Stderr, "looking at offsetlog %s, offsetLog tp: %s\n", offsetTopic, offsetLog.TopicName())

	// find the most recent transaction marker
	txnMarkerTag := txn_data.MarkerTag(offsetLog.TopicNameHash(), parNum)
	var txnMkRawMsg *commtypes.RawMsg
	var err error
	txnMkRawMsg, err = offsetLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, parNum, txnMarkerTag)
	if err != nil {
		return 0, err
	}
	debug.Fprintf(os.Stderr, "offlog got entry off %x, control %v\n",
		txnMkRawMsg.LogSeqNum, txnMkRawMsg.IsControl)
	tag := sharedlog_stream.NameHashWithPartition(offsetLog.TopicNameHash(), parNum)
	debug.Fprintf(os.Stderr, "most recent commit seqNumber 0x%x\n", txnMkRawMsg.LogSeqNum)

	// read the previous item which should record the offset number
	rawMsg, err := offsetLog.ReadBackwardWithTag(ctx, txnMkRawMsg.LogSeqNum, parNum, tag)
	if err != nil {
		return 0, err
	}
	offsetRecord, err := tc.offsetRecordSerde.Decode(rawMsg.Payload)
	if err != nil {
		return 0, err
	}
	return offsetRecord.Offset, nil
}

func (tc *TransactionManager) BeginTransaction(ctx context.Context) error {
	if !txn_data.BEGIN.IsValidPreviousState(tc.currentStatus) {
		debug.Fprintf(os.Stderr, "fail to transition from %v to BEGIN\n", tc.currentStatus)
		return common_errors.ErrInvalidStateTransition
	}
	tc.currentStatus = txn_data.BEGIN
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)

	tc.transactionID += 1
	txnState := txn_data.TxnMetadata{
		State: tc.currentStatus,
	}
	tags := []uint64{sharedlog_stream.NameHashWithPartition(tc.transactionLog.TopicNameHash(), 0), txn_data.BeginTag(tc.transactionLog.TopicNameHash(), 0)}
	off, err := tc.appendToTransactionLog(ctx, txnState, tags)
	if err != nil {
		return err
	}
	tc.transactionID = off
	return nil
}

// second phase of the 2-phase commit protocol
func (tc *TransactionManager) completeTransaction(ctx context.Context, trMark commtypes.EpochMark, trState txn_data.TransactionState) error {
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
		_, err := tc.appendToTransactionLog(tc.backgroundJobCtx, txnMd, nil)
		return err
	})
	return nil
}

func (tc *TransactionManager) CommitTransaction(ctx context.Context) error {
	if !txn_data.PREPARE_COMMIT.IsValidPreviousState(tc.currentStatus) {
		debug.Fprintf(os.Stderr, "Fail to transition from %s to PREPARE_COMMIT\n", tc.currentStatus.String())
		return common_errors.ErrInvalidStateTransition
	}

	// first phase of the commit
	tc.currentStatus = txn_data.PREPARE_COMMIT
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	err := tc.append_pre_state(ctx)
	if err != nil {
		return err
	}
	// second phase of the commit
	err = tc.completeTransaction(ctx, commtypes.EPOCH_END, txn_data.COMPLETE_COMMIT)
	if err != nil {
		return err
	}

	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) AbortTransaction(ctx context.Context) error {
	if !txn_data.PREPARE_ABORT.IsValidPreviousState(tc.currentStatus) {
		debug.Fprintf(os.Stderr, "fail to transition state from %d to PRE_ABORT", tc.currentStatus)
		return common_errors.ErrInvalidStateTransition
	}
	tc.currentStatus = txn_data.PREPARE_ABORT
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	err := tc.append_pre_state(ctx)
	if err != nil {
		return err
	}
	err = tc.completeTransaction(ctx, commtypes.ABORT, txn_data.COMPLETE_ABORT)
	if err != nil {
		return err
	}
	tc.cleanupState()
	return nil
}

func (tc *TransactionManager) cleanupState() {
	tc.currentStatus = txn_data.EMPTY
	// debug.Fprintf(os.Stderr, "Transition to %s\n", tc.currentStatus)
	tc.currentTopicSubstream = make(map[string]map[uint8]struct{})
}

func (tc *TransactionManager) Close() error {
	// wait for all background go rountine to finish
	return tc.backgroundJobErrg.Wait()
}
