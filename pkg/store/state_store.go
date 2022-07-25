package store

import "sharedlog-stream/pkg/exactly_once_intr"

type StateStore interface {
	Name() string
}

type UpdateTrackParFunc interface {
	SetTrackParFunc(exactly_once_intr.TrackProdSubStreamFunc)
}

type ProduceRangeRecording interface {
	GetInitialProdSeqNum() uint64
	GetCurrentProdSeqNum() uint64
	ResetInitialProd()
}
