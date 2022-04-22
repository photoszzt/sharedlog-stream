package processor

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type ProcArgsWithSrcSink interface {
	ProcArgs
	Source() Source
	PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
	ErrChan() chan error
}

type ProcArgs interface {
	ParNum() uint8
	CurEpoch() uint64
	FuncName() string
	RecordFinishFunc() func(ctx context.Context, appId string, instanceId uint8) error
}
