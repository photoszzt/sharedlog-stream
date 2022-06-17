package producer_consumer

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	exactly_once_intr "sharedlog-stream/pkg/transaction/tran_interface"
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
}

type MeteredProducerIntr interface {
	Producer
	MarkFinalOutput()
	StartWarmup()
	GetEventTimeLatency() []int
	GetCount() uint64
}
