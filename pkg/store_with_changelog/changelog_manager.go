package store_with_changelog

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

// a changelog substream has only one producer. Each store would only produce to one substream.
type ChangelogManager[K, V any] struct {
	restoreConsumer *producer_consumer.ShardedSharedLogStreamConsumer
	producer        *producer_consumer.ShardedSharedLogStreamProducer
	msgSerde        commtypes.MessageGSerdeG[K, V]
	changelogIsSrc  bool
	numProduced     uint32
}

func NewChangelogManagerForSrc[K, V any](stream *sharedlog_stream.ShardedSharedLogStream,
	msgSerde commtypes.MessageGSerdeG[K, V], timeout time.Duration,
	serdeFormat commtypes.SerdeFormat, instanceId uint8,
) (*ChangelogManager[K, V], error) {
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(stream,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     timeout,
			SerdeFormat: serdeFormat,
		}, 1, instanceId)
	if err != nil {
		return nil, err
	}
	return &ChangelogManager[K, V]{
		restoreConsumer: consumer,
		producer:        nil,
		changelogIsSrc:  true,
		msgSerde:        msgSerde,
	}, nil
}

func NewChangelogManager[K, V any](stream *sharedlog_stream.ShardedSharedLogStream,
	msgSerde commtypes.MessageGSerdeG[K, V],
	timeout time.Duration,
	flushDuration time.Duration,
	serdeFormat commtypes.SerdeFormat,
	instanceId uint8,
) (*ChangelogManager[K, V], error) {
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumer(stream,
		&producer_consumer.StreamConsumerConfig{
			Timeout:     timeout,
			SerdeFormat: serdeFormat,
		}, 1, instanceId)
	if err != nil {
		return nil, err
	}
	return &ChangelogManager[K, V]{
		restoreConsumer: consumer,
		producer: producer_consumer.NewShardedSharedLogStreamProducer(stream,
			&producer_consumer.StreamSinkConfig{
				FlushDuration: flushDuration,
			}),
		changelogIsSrc: false,
		msgSerde:       msgSerde,
		numProduced:    0,
	}, nil
}

func (cm *ChangelogManager[K, V]) ChangelogIsSrc() bool {
	return cm.changelogIsSrc
}

func (cm *ChangelogManager[K, V]) TopicName() string {
	return cm.restoreConsumer.TopicName()
}

func (cm *ChangelogManager[K, V]) NumPartition() uint8 {
	return cm.restoreConsumer.Stream().NumPartition()
}

func (cm *ChangelogManager[K, V]) ConfigExactlyOnce(
	rem exactly_once_intr.ReadOnlyExactlyOnceManager,
	guarantee exactly_once_intr.GuaranteeMth,
) error {
	if !cm.changelogIsSrc {
		cm.producer.ConfigExactlyOnce(rem, guarantee)
		return cm.restoreConsumer.ConfigExactlyOnce(guarantee)
	}
	return nil
}

func (cm *ChangelogManager[K, V]) Stream() sharedlog_stream.Stream {
	return cm.restoreConsumer.Stream()
}

// when changelog is src, there's nothing to flush
func (cm *ChangelogManager[K, V]) Flush(ctx context.Context) error {
	if !cm.changelogIsSrc {
		// debug.Fprintf(os.Stderr, "flushing changelog manager, current produce: %d\n", cm.numProduced)
		return cm.producer.Flush(ctx)
	}
	return nil
}
func (cm *ChangelogManager[K, V]) Produce(ctx context.Context, msgSer commtypes.MessageSerialized, parNum uint8) error {
	if !cm.changelogIsSrc {
		cm.numProduced += 1
		return cm.producer.ProduceData(ctx, msgSer, parNum)
	}
	return nil
}

func (cm *ChangelogManager[K, V]) ProduceCount() uint32 {
	return cm.numProduced
}

func (cm *ChangelogManager[K, V]) Consume(ctx context.Context, parNum uint8) (commtypes.MsgAndSeqG[K, V], error) {
	rawMsg, err := cm.restoreConsumer.Consume(ctx, parNum)
	if err != nil {
		return commtypes.MsgAndSeqG[K, V]{}, err
	}
	return commtypes.DecodeRawMsgSeqG(rawMsg, cm.msgSerde)
}

func CreateChangelog(env types.Environment, tabName string,
	numPartition uint8, serdeFormat commtypes.SerdeFormat,
) (*sharedlog_stream.ShardedSharedLogStream, error) {
	changelog_name := tabName + "-changelog"
	return sharedlog_stream.NewShardedSharedLogStream(env, changelog_name, numPartition, serdeFormat)
}
