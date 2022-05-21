package source_sink

import (
	"context"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type Sink interface {
	Produce(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
	TopicName() string
	KeySerde() commtypes.Serde
	Flush(ctx context.Context) error
	InitFlushTimer()
	InTransaction(tm tran_interface.ReadOnlyTransactionManager)
	Stream() *sharedlog_stream.ShardedSharedLogStream
}