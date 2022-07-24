package consume_seq_num_manager

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/consume_seq_num_manager/con_types"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

type ConsumeSeqManager struct {
	offsetMarkerSerde commtypes.SerdeG[commtypes.OffsetMarker]
	offsetLogs        map[string]*sharedlog_stream.ShardedSharedLogStream
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
	offsetMarkerSerde, err := commtypes.GetOffsetMarkerSerdeG(serdeFormat)
	if err != nil {
		return nil, err
	}
	return &ConsumeSeqManager{
		offsetMarkerSerde: offsetMarkerSerde,
		offsetLogs:        make(map[string]*sharedlog_stream.ShardedSharedLogStream),
	}, nil
}

func (cm *ConsumeSeqManager) FindLastConsumedSeqNum(ctx context.Context, topicToTrack string, parNum uint8) (uint64, error) {
	offsetTopic := con_types.CONSUMER_OFFSET_LOG_TOPIC_NAME + topicToTrack
	offsetLog := cm.offsetLogs[offsetTopic]
	debug.Assert(offsetTopic == offsetLog.TopicName(), fmt.Sprintf("expected offset log tp: %s, got %s",
		offsetTopic, offsetLog.TopicName()))
	// debug.Fprintf(os.Stderr, "looking at offsetlog %s, offsetLog tp: %s\n", offsetTopic, offsetLog.TopicName())

	// find the most recent transaction marker
	markerTag := txn_data.MarkerTag(offsetLog.TopicNameHash(), parNum)
	var txnMkRawMsg *commtypes.RawMsg
	var err error

	txnMkRawMsg, err = offsetLog.ReadBackwardWithTag(ctx, protocol.MaxLogSeqnum, parNum, markerTag)
	if err != nil {
		return 0, err
	}
	debug.Fprintf(os.Stderr, "CM: offlog got entry off %x, control %v\n",
		txnMkRawMsg.LogSeqNum, txnMkRawMsg.IsControl)
	mark, err := cm.offsetMarkerSerde.Decode(txnMkRawMsg.Payload)
	if err != nil {
		return 0, err
	}
	return mark.Offset, nil
}

func CollectOffsetMarker(consumers []producer_consumer.MeteredConsumerIntr) map[string]commtypes.OffsetMarker {
	ret := make(map[string]commtypes.OffsetMarker)
	for _, consumer := range consumers {
		topic := consumer.TopicName()
		offset := consumer.CurrentConsumedSeqNum()
		offsetTopic := con_types.CONSUMER_OFFSET_LOG_TOPIC_NAME + topic
		tm := commtypes.OffsetMarker{
			Mark:   commtypes.EPOCH_END,
			Offset: offset,
		}
		ret[offsetTopic] = tm
	}
	return ret
}

func (cm *ConsumeSeqManager) Track(ctx context.Context, offsetMarkers map[string]commtypes.OffsetMarker, parNum uint8) error {
	for offsetTopic, tm := range offsetMarkers {
		stream := cm.offsetLogs[offsetTopic]
		encoded, err := cm.offsetMarkerSerde.Encode(tm)
		if err != nil {
			return err
		}
		tag := txn_data.MarkerTag(stream.TopicNameHash(), parNum)
		off, err := stream.PushWithTag(ctx, encoded, parNum, []uint64{tag}, nil,
			sharedlog_stream.ControlRecordMeta, commtypes.EmptyProducerId)
		debug.Fprintf(os.Stderr, "append marker %d to stream %s off %x\n",
			commtypes.EPOCH_END, stream.TopicName(), off)
	}
	return nil
}

func (cm *ConsumeSeqManager) TrackForTest(ctx context.Context, currentConSeqNum map[string]uint64, parNum uint8) error {
	for topic, offset := range currentConSeqNum {
		offsetTopic := con_types.CONSUMER_OFFSET_LOG_TOPIC_NAME + topic
		stream := cm.offsetLogs[offsetTopic]
		tm := commtypes.OffsetMarker{
			Mark:   commtypes.EPOCH_END,
			Offset: offset,
		}
		encoded, err := cm.offsetMarkerSerde.Encode(tm)
		if err != nil {
			return err
		}
		tag := txn_data.MarkerTag(stream.TopicNameHash(), parNum)
		off, err := stream.PushWithTag(ctx, encoded, parNum, []uint64{tag}, nil,
			sharedlog_stream.ControlRecordMeta, commtypes.EmptyProducerId)
		debug.Fprintf(os.Stderr, "append marker %d to stream %s off %x\n",
			commtypes.EPOCH_END, stream.TopicName(), off)
	}
	return nil
}
