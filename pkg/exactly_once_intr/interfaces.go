//go:generate stringer -type=GuaranteeMth
package exactly_once_intr

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
)

type GuaranteeMth uint8

const (
	NO_GUARANTEE GuaranteeMth = iota
	AT_LEAST_ONCE
	TWO_PHASE_COMMIT
	EPOCH_MARK
)

type ReadOnlyExactlyOnceManager interface {
	GetCurrentEpoch() uint16
	GetCurrentTaskId() uint64
	GetProducerId() commtypes.ProducerId
}

type ExactlyOnceManagerLogMonitor interface {
	ErrChan() chan error
	SendQuit()
	// StartMonitorLog(ctx context.Context, cancel context.CancelFunc)
}

type TopicSubstreamTracker interface {
	AddTopicSubstream(ctx context.Context, name string, subNum uint8) error
}

type TrackProdSubStreamFunc func(ctx context.Context,
	topicName string,
	substreamId uint8,
) error

type FlushCallbackFunc func(ctx context.Context) error

func DefaultTrackProdSubstreamFunc(ctx context.Context,
	topicName string,
	substreamId uint8,
) error {
	return nil
}

type RecordPrevInstanceFinishFunc func(ctx context.Context, appId string, instanceID uint8) error

func DefaultRecordPrevInstanceFinishFunc(ctx context.Context,
	appId string, instanceId uint8,
) error {
	debug.Fprintf(os.Stderr, "Running empty default record finish func\n")
	return nil
}
