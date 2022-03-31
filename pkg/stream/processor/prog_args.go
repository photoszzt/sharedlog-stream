package processor

import "context"

type ProcArgsWithSrcSink interface {
	ProcArgs
	Source() Source
	Sink() Sink
}

type ProcArgs interface {
	ParNum() uint8
	CurEpoch() uint64
	FuncName() string
	RecordFinishFunc() func(ctx context.Context, appId string, instanceId uint8) error
}
