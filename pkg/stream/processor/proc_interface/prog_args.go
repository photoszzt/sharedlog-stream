package proc_interface

import (
	"context"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type ProcArgsWithSrcSink interface {
	ProcArgs
	Source() source_sink.Source
	PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
}

type ProcArgs interface {
	ParNum() uint8
	CurEpoch() uint64
	FuncName() string
	RecordFinishFunc() tran_interface.RecordPrevInstanceFinishFunc
}
