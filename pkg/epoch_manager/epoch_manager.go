package epoch_manager

import (
	"context"
	"math"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sync"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

const (
	EPOCH_LOG_TOPIC_NAME = "__epoch_log"
)

type EpochManager struct {
	epochMetaSerde commtypes.Serde
	env            types.Environment

	tpMapMu               sync.Mutex
	currentTopicSubstream map[string]map[uint8]struct{}

	epochLog          *sharedlog_stream.SharedLogStream
	epochLogMarkerTag uint64
	epochMngrName     string
	commtypes.ProducerId
	errChan  chan error
	quitChan chan struct{}
}

func NewEpochManager(env types.Environment, epochMngrName string,
	serdeFormat commtypes.SerdeFormat,
) (*EpochManager, error) {
	log, err := sharedlog_stream.NewSharedLogStream(env,
		EPOCH_LOG_TOPIC_NAME+"_"+epochMngrName, serdeFormat)
	if err != nil {
		return nil, err
	}
	epochMetaSerde, err := commtypes.GetEpochMarkerSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	return &EpochManager{
		epochMngrName:         epochMngrName,
		env:                   env,
		ProducerId:            commtypes.NewProducerId(),
		currentTopicSubstream: make(map[string]map[uint8]struct{}),
		epochLog:              log,
		epochMetaSerde:        epochMetaSerde,
		epochLogMarkerTag:     txn_data.MarkerTag(log.TopicNameHash(), 0),
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
	fenceTag := txn_data.FenceTag(em.epochLog.TopicNameHash(), 0)
	for {
		select {
		case <-quit:
			return
		default:
		}
		rawMsg, err := em.epochLog.ReadNextWithTag(ctx, 0, fenceTag)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				continue
			}
			errc <- err
			return
		} else {
			epochMetaTmp, err := em.epochMetaSerde.Decode(rawMsg.Payload)
			if err != nil {
				errc <- err
				return
			}
			epochMeta := epochMetaTmp.(commtypes.EpochMarker)
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
	meta *commtypes.EpochMarker, tags []uint64, additionalTopic []string,
) (uint64, error) {
	encoded, err := em.epochMetaSerde.Encode(meta)
	if err != nil {
		return 0, err
	}
	debug.Assert(tags != nil, "tags should not be null")
	producerId := commtypes.ProducerId{
		TaskId:        em.TaskId,
		TaskEpoch:     em.TaskEpoch,
		TransactionID: 0,
	}
	return em.epochLog.PushWithTag(ctx, encoded, 0, tags, additionalTopic,
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

func (em *EpochManager) MarkEpoch(ctx context.Context,
	consumeSeqNums map[string]uint64,
	producers []producer_consumer.MeteredProducerIntr,
) error {
	outputRanges := make(map[string]map[uint8]commtypes.ProduceRange)
	for _, producer := range producers {
		topicName := producer.TopicName()
		parSet, ok := em.currentTopicSubstream[producer.TopicName()]
		if ok {
			ranges, ok := outputRanges[topicName]
			if !ok {
				ranges = make(map[uint8]commtypes.ProduceRange)
			}
			for subNum := range parSet {
				ranges[subNum] = commtypes.ProduceRange{
					Start: producer.GetInitialProdSeqNum(subNum),
					End:   producer.GetCurrentProdSeqNum(subNum),
				}
			}
			outputRanges[topicName] = ranges
		}
	}
	epochMeta := commtypes.EpochMarker{
		Mark:         commtypes.EPOCH_END,
		ConSeqNums:   consumeSeqNums,
		OutputRanges: outputRanges,
	}
	tags := []uint64{em.epochLogMarkerTag}
	var additionalTopic []string
	for tp, parSet := range em.currentTopicSubstream {
		additionalTopic = append(additionalTopic, tp)
		for sub := range parSet {
			tag := txn_data.MarkerTag(sharedlog_stream.NameHash(tp), sub)
			tags = append(tags, tag)
		}
	}
	_, err := em.appendToEpochLog(ctx, &epochMeta, tags, additionalTopic)
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
		sharedlog_stream.NameHashWithPartition(em.epochLog.TopicNameHash(), 0),
		txn_data.FenceTag(em.epochLog.TopicNameHash(), 0),
	}
	_, err = em.appendToEpochLog(ctx, &meta, tags, nil)
	if err != nil {
		return nil, 0, err
	}
	return recentMeta, metaMsg.LogSeqNum, nil
}

func (em *EpochManager) getMostRecentCommitEpoch(ctx context.Context) (*commtypes.EpochMarker, *commtypes.RawMsg, error) {
	rawMsg, err := em.epochLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, 0, em.epochLogMarkerTag)
	if err != nil {
		if common_errors.IsStreamEmptyError(err) {
			return nil, nil, nil
		}
		return nil, nil, err
	}
	val, err := em.epochMetaSerde.Decode(rawMsg.Payload)
	if err != nil {
		return nil, nil, err
	}
	meta := val.(commtypes.EpochMarker)
	return &meta, rawMsg, nil
}

func (em *EpochManager) FindMostRecentEpochMetaThatHasConsumed(
	ctx context.Context, seqNum uint64,
) (*commtypes.EpochMarker, error) {
	for {
		rawMsg, err := em.epochLog.ReadBackwardWithTag(ctx, seqNum, 0, em.epochLogMarkerTag)
		if err != nil {
			if common_errors.IsStreamEmptyError(err) {
				return nil, nil
			}
			return nil, err
		}
		val, err := em.epochMetaSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, err
		}
		meta := val.(commtypes.EpochMarker)
		if meta.ConSeqNums != nil && len(meta.ConSeqNums) != 0 {
			return &meta, nil
		}
	}
}

func (em *EpochManager) cleanupState() {
	em.currentTopicSubstream = make(map[string]map[uint8]struct{})
}
