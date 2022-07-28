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
	KeyEncoder() commtypes.Encoder
	Flush(ctx context.Context) error
	// InitFlushTimer()
	ConfigExactlyOnce(rem exactly_once_intr.ReadOnlyExactlyOnceManager,
		guarantee exactly_once_intr.GuaranteeMth)
	Stream() sharedlog_stream.Stream
	GetInitialProdSeqNum(substreamNum uint8) uint64
	GetCurrentProdSeqNum(substreamNum uint8) uint64
	ResetInitialProd()
	// Lock()
	// Unlock()
}

type MeteredProducerIntr interface {
	Producer
	MarkFinalOutput()
	IsFinalOutput() bool
	StartWarmup()
	// GetEventTimeLatency() []int
	GetCount() uint64
	NumCtrlMsg() uint64
	PrintRemainingStats()
}
