package producer_consumer

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
)

type Consumer interface {
	// Consume gets the next commtypes.Message from the source
	Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error)
	SetCursor(cursor uint64, parNum uint8)
	TopicName() string
	Name() string
	SetName(string)
	Stream() sharedlog_stream.Stream
	ConfigExactlyOnce(serdeFormat commtypes.SerdeFormat, guarantee exactly_once_intr.GuaranteeMth) error
	SetInitialSource(initial bool)
	IsInitialSource() bool
	// Lock()
	// Unlock()
	RecordCurrentConsumedSeqNum(seqNum uint64)
	CurrentConsumedSeqNum() uint64
}

type MeteredConsumerIntr interface {
	Consumer
	StartWarmup()
	GetCount() uint64
	InnerSource() Consumer
	ExtractProduceToConsumeTime(msg *commtypes.Message)
}
