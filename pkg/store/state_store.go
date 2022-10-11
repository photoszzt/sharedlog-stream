package store

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/exactly_once_intr"
)

type StateStore interface {
	Name() string
}

type UpdateTrackParFunc interface {
	SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc)
}

type ProduceRangeRecording interface {
	GetInitialProdSeqNum() uint64
	ResetInitialProd()
}

type CachedStateStore[K, V any] interface {
	SetFlushCallback(func(ctx context.Context, msg commtypes.MessageG[K, commtypes.ChangeG[V]]) error)
}

type CachedWindowStateStore[K, V any] interface {
	SetFlushCallback(func(ctx context.Context, msg commtypes.MessageG[commtypes.WindowedKeyG[K], commtypes.ChangeG[V]]) error)
}
