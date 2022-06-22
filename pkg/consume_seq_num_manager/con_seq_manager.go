package consume_seq_num_manager

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/consume_seq_num_manager/con_types"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"
	"sync"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/sync/errgroup"
)

type ConsumeSeqManager struct {
	offsetRecordSerde commtypes.Serde
	epochMarkerSerde  commtypes.Serde
	mapMu             sync.Mutex
	curConsumePar     map[string]map[uint8]struct{}
	offsetLogs        map[string]*sharedlog_stream.ShardedSharedLogStream
}

func (cm *ConsumeSeqManager) AddTopicTrackConsumedSeqs(ctx context.Context, topicToTrack string, partitions []uint8) {
	offsetTopic := con_types.OffsetTopic(topicToTrack)
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
	offsetTopic := con_types.OffsetTopic(topicToTrack)
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
	offsetRecordSerde, err := txn_data.GetOffsetRecordSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	txnMarkerSerde, err := commtypes.GetEpochMarkerSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	return &ConsumeSeqManager{
		epochMarkerSerde:  txnMarkerSerde,
		offsetRecordSerde: offsetRecordSerde,
		offsetLogs:        make(map[string]*sharedlog_stream.ShardedSharedLogStream),
		curConsumePar:     make(map[string]map[uint8]struct{}),
	}, nil
}

func (cm *ConsumeSeqManager) AppendConsumedSeqNum(ctx context.Context, consumedSeqNumConfigs []con_types.ConsumedSeqNumConfig) error {
	for _, consumedSeqNumConfig := range consumedSeqNumConfigs {
		offsetTopic := con_types.OffsetTopic(consumedSeqNumConfig.TopicToTrack)
		offsetLog := cm.offsetLogs[offsetTopic]
		offsetRecord := txn_data.OffsetRecord{
			Offset: consumedSeqNumConfig.ConsumedSeqNum,
		}
		encoded, err := cm.offsetRecordSerde.Encode(&offsetRecord)
		if err != nil {
			return err
		}

		_, err = offsetLog.Push(ctx, encoded, consumedSeqNumConfig.Partition,
			sharedlog_stream.SingleDataRecordMeta, commtypes.EmptyProducerId)
		if err != nil {
			return err
		}
		debug.Fprintf(os.Stderr, "consumed offset 0x%x for %s\n",
			consumedSeqNumConfig.ConsumedSeqNum, consumedSeqNumConfig.TopicToTrack)
	}
	return nil
}

func (cm *ConsumeSeqManager) FindLastConsumedSeqNum(ctx context.Context, topicToTrack string, parNum uint8) (uint64, error) {
	offsetTopic := con_types.CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	offsetLog := cm.offsetLogs[offsetTopic]
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
	debug.Fprintf(os.Stderr, "CM: offlog got entry off %x, control %v\n",
		txnMkRawMsg.LogSeqNum, txnMkRawMsg.IsControl)
	tag := sharedlog_stream.NameHashWithPartition(offsetLog.TopicNameHash(), parNum)

	// read the previous item which should record the offset number
	rawMsg, err := offsetLog.ReadBackwardWithTag(ctx, txnMkRawMsg.LogSeqNum, parNum, tag)
	if err != nil {
		return 0, err
	}
	return rawMsg.LogSeqNum, nil
}

func (cm *ConsumeSeqManager) Track(ctx context.Context) error {
	tm := commtypes.EpochMarker{
		Mark: commtypes.EPOCH_END,
	}
	encoded, err := cm.epochMarkerSerde.Encode(&tm)
	if err != nil {
		return err
	}
	g, ectx := errgroup.WithContext(ctx)
	for topic, partitions := range cm.curConsumePar {
		stream := cm.offsetLogs[topic]
		err := stream.Flush(ctx, commtypes.EmptyProducerId)
		if err != nil {
			return err
		}
		for par := range partitions {
			parNum := par
			g.Go(func() error {
				tag := txn_data.MarkerTag(stream.TopicNameHash(), parNum)
				off, err := stream.PushWithTag(ectx, encoded, parNum, []uint64{tag}, nil,
					sharedlog_stream.ControlRecordMeta, commtypes.EmptyProducerId)
				debug.Fprintf(os.Stderr, "append marker %d to stream %s off %x\n",
					commtypes.EPOCH_END, stream.TopicName(), off)
				return err
			})
		}
	}
	err = g.Wait()
	if err != nil {
		return err
	}
	return nil
}
