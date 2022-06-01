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

type BaseProcArgsWithSrcSink struct {
	src source_sink.Source
	BaseProcArgsWithSink
}

func NewBaseProcArgsWithSrcSink(src source_sink.Source, sinks []source_sink.Sink, funcName string,
	curEpoch uint64, parNum uint8,
) BaseProcArgsWithSrcSink {
	return BaseProcArgsWithSrcSink{
		BaseProcArgsWithSink: NewBaseProcArgsWithSink(sinks, funcName, curEpoch, parNum),
		src:                  src,
	}
}

func (pa *BaseProcArgsWithSrcSink) Source() source_sink.Source {
	return pa.src
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
	SetRecordFinishFunc(recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc)
	TrackParFunc() tran_interface.TrackKeySubStreamFunc
	SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc)
}

type BaseProcArgsBuilder struct {
	bp *BaseProcArgs
}

type SetFuncName interface {
	FuncName(string) SetCurEpoch
}

type SetCurEpoch interface {
	CurEpoch(uint64) SetParNum
}

type SetParNum interface {
	ParNum(uint8) BuildProcArgs
}

type BuildProcArgs interface {
	Build() ProcArgs
	TrackParFunc(tran_interface.TrackKeySubStreamFunc) BuildProcArgs
	RecordFinishFunc(tran_interface.RecordPrevInstanceFinishFunc) BuildProcArgs
}

func NewBaseProcArgsBuilder() SetFuncName {
	return &BaseProcArgsBuilder{
		bp: &BaseProcArgs{},
	}
}

func (b *BaseProcArgsBuilder) FuncName(funcName string) SetCurEpoch {
	b.bp.funcName = funcName
	return b
}
func (b *BaseProcArgsBuilder) CurEpoch(curEpoch uint64) SetParNum {
	b.bp.curEpoch = curEpoch
	return b
}
func (b *BaseProcArgsBuilder) ParNum(parNum uint8) BuildProcArgs {
	b.bp.parNum = parNum
	return b
}
func (b *BaseProcArgsBuilder) Build() ProcArgs {
	if b.bp.trackParFunc == nil {
		b.bp.trackParFunc = tran_interface.DefaultTrackSubstreamFunc
	}
	if b.bp.recordFinishFunc == nil {
		b.bp.recordFinishFunc = tran_interface.DefaultRecordPrevInstanceFinishFunc
	}
	return b.bp
}
func (b *BaseProcArgsBuilder) TrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) BuildProcArgs {
	b.bp.trackParFunc = trackParFunc
	return b
}
func (b *BaseProcArgsBuilder) RecordFinishFunc(recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) BuildProcArgs {
	b.bp.recordFinishFunc = recordFinishFunc
	return b
}

type BaseProcArgs struct {
	recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc
	trackParFunc     tran_interface.TrackKeySubStreamFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func NewBaseProcArgs(funcName string, curEpoch uint64, parNum uint8) BaseProcArgs {
	return BaseProcArgs{
		recordFinishFunc: tran_interface.DefaultRecordPrevInstanceFinishFunc,
		trackParFunc:     tran_interface.DefaultTrackSubstreamFunc,
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

func (pa *BaseProcArgs) TrackParFunc() tran_interface.TrackKeySubStreamFunc {
	return pa.trackParFunc
}

func (pa *BaseProcArgs) SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) {
	pa.trackParFunc = trackParFunc
}

type ProcessMsgFunc func(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error
