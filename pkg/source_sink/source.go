package source_sink

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
)

type Source interface {
	// Consume gets the next commtypes.Message from the source
	Consume(ctx context.Context, parNum uint8) (*commtypes.MsgAndSeqs, error)
	SetCursor(cursor uint64, parNum uint8)
	TopicName() string
	Stream() store.Stream
	InTransaction(serdeFormat commtypes.SerdeFormat) error
	SetInitialSource(initial bool)
	IsInitialSource() bool
	KVMsgSerdes() commtypes.KVMsgSerdes
}

type MeteredSourceIntr interface {
	Source
	StartWarmup()
	GetCount() uint64
	InnerSource() Source
}
