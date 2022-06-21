package producer_consumer

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	exactly_once_intr "sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
)

type Producer interface {
	Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
	TopicName() string
	Name() string
	SetName(string)
	KeySerde() commtypes.Serde
	Flush(ctx context.Context) error
	InitFlushTimer()
	ConfigExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
		guarantee exactly_once_intr.GuaranteeMth)
	Stream() *sharedlog_stream.ShardedSharedLogStream
	GetInitialProdSeqNum(substreamNum uint8) uint64
	GetCurrentProdSeqNum(substreamNum uint8) uint64
	ResetInitialProd()
}

type MeteredProducerIntr interface {
	Producer
	MarkFinalOutput()
	StartWarmup()
	GetEventTimeLatency() []int
	GetCount() uint64
}
