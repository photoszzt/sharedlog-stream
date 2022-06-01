package tran_interface

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type ReadOnlyTransactionManager interface {
	GetCurrentEpoch() uint16
	GetCurrentTaskId() uint64
	GetTransactionID() uint64
}

type TrackKeySubStreamFunc func(ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	topicName string,
	substreamId uint8,
) error

func DefaultTrackSubstreamFunc(ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	topicName string,
	substreamId uint8,
) error {
	return nil
}

type RecordPrevInstanceFinishFunc func(ctx context.Context, appId string, instanceID uint8) error

func DefaultRecordPrevInstanceFinishFunc(ctx context.Context,
	appId string, instanceId uint8,
) error {
	return nil
}
