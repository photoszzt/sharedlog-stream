package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/exactly_once_intr"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

// a changelog substream has only one producer. Each store would only produce to one substream.
type ChangelogManager struct {
	restoreConsumer *producer_consumer.ShardedSharedLogStreamConsumer
	producer        *producer_consumer.ShardedSharedLogStreamProducer
	stream          *sharedlog_stream.ShardedSharedLogStream
	changelogIsSrc  bool
}

func NewChangelogManagerForSrc(stream *sharedlog_stream.ShardedSharedLogStream,
	kvMsgSerdes commtypes.KVMsgSerdes, timeout time.Duration,
) *ChangelogManager {
	return &ChangelogManager{
		restoreConsumer: producer_consumer.NewShardedSharedLogStreamConsumer(stream,
			&producer_consumer.StreamConsumerConfig{
				KVMsgSerdes: kvMsgSerdes,
				Timeout:     timeout,
			}),
		producer:       nil,
		changelogIsSrc: true,
	}
}

func NewChangelogManager(stream *sharedlog_stream.ShardedSharedLogStream,
	kvMsgSerdes commtypes.KVMsgSerdes,
	timeout time.Duration,
	flushDuration time.Duration,
) *ChangelogManager {
	return &ChangelogManager{
		restoreConsumer: producer_consumer.NewShardedSharedLogStreamConsumer(stream,
			&producer_consumer.StreamConsumerConfig{
				KVMsgSerdes: kvMsgSerdes,
				Timeout:     timeout,
			}),
		producer: producer_consumer.NewShardedSharedLogStreamProducer(stream,
			&producer_consumer.StreamSinkConfig{
				KVMsgSerdes:   kvMsgSerdes,
				FlushDuration: flushDuration,
			}),
		changelogIsSrc: false,
	}
}

func (cm *ChangelogManager) ChangelogIsSrc() bool {
	return cm.changelogIsSrc
}

func (cm *ChangelogManager) TopicName() string {
	return cm.restoreConsumer.TopicName()
}

func (cm *ChangelogManager) NumPartition() uint8 {
	return cm.restoreConsumer.Stream().NumPartition()
}

func (cm *ChangelogManager) ConfigExactlyOnce(
	rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
	serdeFormat commtypes.SerdeFormat,
) error {
	if !cm.changelogIsSrc {
		cm.producer.ConfigExactlyOnce(rem, guarantee)
		return cm.restoreConsumer.ConfigExactlyOnce(serdeFormat, guarantee)
	}
	return nil
}

func (cm *ChangelogManager) Stream() *sharedlog_stream.ShardedSharedLogStream {
	return cm.stream
}

func (cm *ChangelogManager) Flush(ctx context.Context) error {
	return cm.producer.FlushNoLock(ctx)
}
func (cm *ChangelogManager) Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	return cm.producer.Produce(ctx, msg, parNum, isControl)
}

func (cm *ChangelogManager) Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error) {
	return cm.restoreConsumer.Consume(ctx, parNum)
}

func CreateChangelog(env types.Environment, tabName string,
	numPartition uint8, serdeFormat commtypes.SerdeFormat,
) (*sharedlog_stream.ShardedSharedLogStream, error) {
	changelog_name := tabName + "-changelog"
	return sharedlog_stream.NewShardedSharedLogStream(env, changelog_name, numPartition, serdeFormat)
}
