package source_sink

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type Sink interface {
	Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
	TopicName() string
	Name() string
	SetName(string)
	KeySerde() commtypes.Serde
	Flush(ctx context.Context) error
	InitFlushTimer()
	InTransaction(tm tran_interface.ReadOnlyExactlyOnceManager)
	Stream() *sharedlog_stream.ShardedSharedLogStream
}

type MeteredSink interface {
	Sink
	MarkFinalOutput()
	StartWarmup()
	GetEventTimeLatency() []int
	GetCount() uint64
}
