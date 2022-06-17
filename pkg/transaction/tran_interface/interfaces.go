package tran_interface

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
)

type GuaranteeMth uint8

const (
	AT_LEAST_ONCE    GuaranteeMth = 0
	TWO_PHASE_COMMIT GuaranteeMth = 1
	EPOCH_MARK       GuaranteeMth = 2
)

type ReadOnlyExactlyOnceManager interface {
	GetCurrentEpoch() uint16
	GetCurrentTaskId() uint64
	GetTransactionID() uint64
	GetProducerId() ProducerId
}

type TrackProdSubStreamFunc func(ctx context.Context,
	key interface{},
	keySerde commtypes.Serde,
	topicName string,
	substreamId uint8,
) error

func DefaultTrackProdSubstreamFunc(ctx context.Context,
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
