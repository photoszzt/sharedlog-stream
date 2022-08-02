package epoch_manager

import (
	"context"
	"math"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/data_structure"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils/syncutils"
	"time"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

const (
	EPOCH_LOG_TOPIC_NAME = "__epoch_log"
)

type EpochManager struct {
	epochMetaSerde commtypes.SerdeG[commtypes.EpochMarker]
	env            types.Environment

	tpMapMu               syncutils.Mutex
	currentTopicSubstream map[string]data_structure.Uint8Set

	// two different meta data class
	epochLogForRead   *sharedlog_stream.SharedLogStream
	epochLogForWrite  *sharedlog_stream.SharedLogStream
	epochLogMarkerTag uint64
	epochMngrName     string
	commtypes.ProducerId
	errChan  chan error
	quitChan chan struct{}
}

func NewEpochManager(env types.Environment, epochMngrName string,
	serdeFormat commtypes.SerdeFormat,
) (*EpochManager, error) {
	logForRead, err := sharedlog_stream.NewSharedLogStream(env,
		EPOCH_LOG_TOPIC_NAME+"_"+epochMngrName, serdeFormat)
	if err != nil {
		return nil, err
	}
	logForWrite, err := sharedlog_stream.NewSharedLogStream(env,
		EPOCH_LOG_TOPIC_NAME+"_"+epochMngrName, serdeFormat)
	if err != nil {
		return nil, err
	}
	epochMetaSerde, err := commtypes.GetEpochMarkerSerdeG(serdeFormat)
	if err != nil {
		return nil, err
	}
	return &EpochManager{
		epochMngrName:         epochMngrName,
		env:                   env,
		ProducerId:            commtypes.NewProducerId(),
		currentTopicSubstream: make(map[string]data_structure.Uint8Set),
		epochLogForRead:       logForRead,
		epochLogForWrite:      logForWrite,
		epochMetaSerde:        epochMetaSerde,
		epochLogMarkerTag:     txn_data.MarkerTag(logForRead.TopicNameHash(), 0),
	}, nil
}

func (em *EpochManager) SendQuit() {
	em.quitChan <- struct{}{}
}

func (em *EpochManager) ErrChan() chan error {
	return em.errChan
}

func (em *EpochManager) GetEpochManagerName() string {
	return em.epochMngrName
}

func (em *EpochManager) monitorEpochLog(ctx context.Context,
	quit chan struct{}, errc chan error, dcancel context.CancelFunc,
) {
	fenceTag := txn_data.FenceTag(em.epochLogForRead.TopicNameHash(), 0)
	for {
		select {
		case <-quit:
			return
		default:
		}
		rawMsg, err := em.epochLogForRead.ReadNextWithTag(ctx, 0, fenceTag)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				time.Sleep(time.Duration(1) * time.Second)
				continue
			}
			errc <- err
			return
		} else {
			epochMeta, err := em.epochMetaSerde.Decode(rawMsg.Payload)
			if err != nil {
				errc <- err
				return
			}
			if epochMeta.Mark != commtypes.FENCE {
				panic("mark should be fence")
			}
			if rawMsg.ProdId.TaskId == em.TaskId && rawMsg.ProdId.TaskEpoch > em.GetCurrentEpoch() {
				// I'm the zoombie
				dcancel()
				errc <- nil
				return
			}
		}
	}
}

func (em *EpochManager) StartMonitorLog(
	ctx context.Context, dcancel context.CancelFunc,
) {
	em.quitChan = make(chan struct{})
	em.errChan = make(chan error, 1)
	go em.monitorEpochLog(ctx, em.quitChan, em.errChan, dcancel)
}

func (em *EpochManager) appendToEpochLog(ctx context.Context,
	meta commtypes.EpochMarker, tags []uint64, additionalTopic []string,
) (uint64, error) {
	encoded, err := em.epochMetaSerde.Encode(meta)
	if err != nil {
		return 0, err
	}
	// debug.Fprintf(os.Stderr, "encoded epochMeta: %v\n", string(encoded))
	debug.Assert(tags != nil, "tags should not be null")
	producerId := commtypes.ProducerId{
		TaskId:        em.TaskId,
		TaskEpoch:     em.TaskEpoch,
		TransactionID: 0,
	}
	return em.epochLogForWrite.PushWithTag(ctx, encoded, 0, tags, additionalTopic,
		sharedlog_stream.ControlRecordMeta, producerId)
}

func (em *EpochManager) AddTopicSubstream(ctx context.Context,
	topic string, substreamNum uint8,
) error {
	em.tpMapMu.Lock()
	defer em.tpMapMu.Unlock()
	parSet, ok := em.currentTopicSubstream[topic]
	if !ok {
		parSet = make(map[uint8]struct{})
	}
	_, ok = parSet[substreamNum]
	if !ok {
		parSet[substreamNum] = struct{}{}
	}
	em.currentTopicSubstream[topic] = parSet
	return nil
}

func GenEpochMarker(
	ctx context.Context,
	em *EpochManager,
	consumers []producer_consumer.MeteredConsumerIntr,
	producers []producer_consumer.MeteredProducerIntr,
	kvTabs []store.KeyValueStoreOpWithChangelog,
	winTabs []store.WindowStoreOpWithChangelog,
) (commtypes.EpochMarker, error) {
	outputRanges := make(map[string][]commtypes.ProduceRange)
	for _, producer := range producers {
		// err := producer.Flush(ctx)
		// if err != nil {
		// 	return commtypes.EpochMarker{}, err
		// }
		topicName := producer.TopicName()
		parSet, ok := em.currentTopicSubstream[producer.TopicName()]
		if ok {
			ranges := make([]commtypes.ProduceRange, 0, 4)
			for subNum := range parSet {
				ranges = append(ranges, commtypes.ProduceRange{
					SubStreamNum: subNum,
					Start:        producer.GetInitialProdSeqNum(subNum),
					End:          producer.GetCurrentProdSeqNum(subNum),
				})
			}
			outputRanges[topicName] = ranges
		}
	}
	for _, kvTab := range kvTabs {
		if !kvTab.ChangelogIsSrc() {
			// err := kvTab.FlushChangelog(ctx)
			// if err != nil {
			// 	return commtypes.EpochMarker{}, err
			// }
			ranges := []commtypes.ProduceRange{
				{
					SubStreamNum: kvTab.SubstreamNum(),
					Start:        kvTab.GetInitialProdSeqNum(),
					End:          kvTab.GetCurrentProdSeqNum(),
				},
			}
			outputRanges[kvTab.ChangelogTopicName()] = ranges
		}
	}
	for _, winTab := range winTabs {
		// err := winTab.FlushChangelog(ctx)
		// if err != nil {
		// 	return commtypes.EpochMarker{}, err
		// }
		ranges := []commtypes.ProduceRange{
			{
				SubStreamNum: winTab.SubstreamNum(),
				Start:        winTab.GetInitialProdSeqNum(),
				End:          winTab.GetCurrentProdSeqNum(),
			},
		}
		outputRanges[winTab.ChangelogTopicName()] = ranges
	}
	consumeSeqNums := make(map[string]uint64)
	for _, consumer := range consumers {
		consumeSeqNums[consumer.TopicName()] = consumer.CurrentConsumedSeqNum()
	}
	epochMeta := commtypes.EpochMarker{
		Mark:         commtypes.EPOCH_END,
		ConSeqNums:   consumeSeqNums,
		OutputRanges: outputRanges,
	}
	return epochMeta, nil
}

func (em *EpochManager) GenTagsAndTopicsForEpochMarker() ([]uint64, []string) {
	tags := []uint64{em.epochLogMarkerTag}
	// debug.Fprintf(os.Stderr, "marker with tag: 0x%x\n", em.epochLogMarkerTag)
	var additionalTopic []string
	for tp, parSet := range em.currentTopicSubstream {
		additionalTopic = append(additionalTopic, tp)
		nameHash := hashfuncs.NameHash(tp)
		for sub := range parSet {
			tag := sharedlog_stream.NameHashWithPartition(nameHash, sub)
			// debug.Fprintf(os.Stderr, "marker with tag: 0x%x\n", tag)
			tags = append(tags, tag)
		}
	}
	return tags, additionalTopic
}

func (em *EpochManager) MarkEpoch(ctx context.Context,
	epochMeta commtypes.EpochMarker, tags []uint64, additionalTopic []string,
) error {
	// debug.Fprintf(os.Stderr, "epochMeta to push: %+v\n", epochMeta)
	_, err := em.appendToEpochLog(ctx, epochMeta, tags, additionalTopic)
	return err
}

func (em *EpochManager) Init(ctx context.Context) (*commtypes.EpochMarker, uint64, error) {
	recentMeta, metaMsg, err := em.getMostRecentCommitEpoch(ctx)
	if err != nil {
		return nil, 0, err
	}
	if recentMeta == nil {
		em.InitTaskId(em.env)
		em.TaskEpoch = 0
	} else {
		em.TaskEpoch = metaMsg.ProdId.TaskEpoch
		em.TaskId = metaMsg.ProdId.TaskId
	}
	if recentMeta != nil && metaMsg.ProdId.TaskEpoch == math.MaxUint16 {
		em.InitTaskId(em.env)
		em.TaskEpoch = 0
	}
	meta := commtypes.EpochMarker{
		Mark: commtypes.FENCE,
	}
	tags := []uint64{
		sharedlog_stream.NameHashWithPartition(em.epochLogForWrite.TopicNameHash(), 0),
		txn_data.FenceTag(em.epochLogForWrite.TopicNameHash(), 0),
	}
	_, err = em.appendToEpochLog(ctx, meta, tags, nil)
	if err != nil {
		return nil, 0, err
	}
	if recentMeta != nil {
		return recentMeta, metaMsg.LogSeqNum, nil
	} else {
		return recentMeta, 0, nil
	}
}

func (em *EpochManager) getMostRecentCommitEpoch(ctx context.Context) (*commtypes.EpochMarker, *commtypes.RawMsg, error) {
	rawMsg, err := em.epochLogForWrite.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, 0, em.epochLogMarkerTag)
	if err != nil {
		if common_errors.IsStreamEmptyError(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	meta, err := em.epochMetaSerde.Decode(rawMsg.Payload)
	if err != nil {
		return nil, nil, err
	}
	return &meta, rawMsg, nil
}

func (em *EpochManager) FindMostRecentEpochMetaThatHasConsumed(
	ctx context.Context, seqNum uint64,
) (*commtypes.EpochMarker, error) {
	for {
		rawMsg, err := em.epochLogForWrite.ReadBackwardWithTag(ctx, seqNum, 0, em.epochLogMarkerTag)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				return nil, nil
			}
			return nil, err
		}
		meta, err := em.epochMetaSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, err
		}
		if meta.ConSeqNums != nil && len(meta.ConSeqNums) != 0 {
			return &meta, nil
		}
	}
}

func CleanupState(em *EpochManager, producers []producer_consumer.MeteredProducerIntr,
	kvTabs []store.KeyValueStoreOpWithChangelog, winTabs []store.WindowStoreOpWithChangelog,
) {
	em.currentTopicSubstream = make(map[string]data_structure.Uint8Set)
	for _, producer := range producers {
		producer.ResetInitialProd()
	}
	for _, kvTab := range kvTabs {
		if !kvTab.ChangelogIsSrc() {
			kvTab.ResetInitialProd()
		}
	}
	for _, winTab := range winTabs {
		winTab.ResetInitialProd()
	}
}
