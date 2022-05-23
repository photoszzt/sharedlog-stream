package proc_interface

import (
	"context"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type ProcArgsWithSrcSink interface {
	ProcArgsWithSink
	Source() source_sink.Source
}

type ProcArgsWithSink interface {
	ProcArgs
	Sinks() []source_sink.Sink
	FlushAndPushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
}

type BaseProcArgsWithSink struct {
	sinks []source_sink.Sink
	BaseProcArgs
}

func NewBaseProcArgsWithSink(sinks []source_sink.Sink, funcName string, curEpoch uint64, parNum uint8) BaseProcArgsWithSink {
	return BaseProcArgsWithSink{
		sinks:        sinks,
		BaseProcArgs: NewBaseProcArgs(funcName, curEpoch, parNum),
	}
}

func (pa *BaseProcArgsWithSink) FlushAndPushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	for _, sink := range pa.sinks {
		err := sink.Produce(ctx, msg, parNum, isControl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pa *BaseProcArgsWithSink) Sinks() []source_sink.Sink {
	return pa.sinks
}

type ProcArgs interface {
	ParNum() uint8
	CurEpoch() uint64
	FuncName() string
	RecordFinishFunc() tran_interface.RecordPrevInstanceFinishFunc
}

type BaseProcArgs struct {
	recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func NewBaseProcArgs(funcName string, curEpoch uint64, parNum uint8) BaseProcArgs {
	return BaseProcArgs{
		recordFinishFunc: tran_interface.DefaultRecordPrevInstanceFinishFunc,
		funcName:         funcName,
		curEpoch:         curEpoch,
		parNum:           parNum,
	}
}

func (pa *BaseProcArgs) ParNum() uint8 {
	return pa.parNum
}

func (pa *BaseProcArgs) FuncName() string {
	return pa.funcName
}

func (pa *BaseProcArgs) CurEpoch() uint64 {
	return pa.curEpoch
}

func (pa *BaseProcArgs) RecordFinishFunc() tran_interface.RecordPrevInstanceFinishFunc {
	return pa.recordFinishFunc
}

func (pa *BaseProcArgs) SetRecordFinishFunc(recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
	pa.recordFinishFunc = recordFinishFunc
}
