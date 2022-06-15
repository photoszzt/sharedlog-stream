package proc_interface

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type ExecutionContext interface {
	ProcArgs
	SourcesSinks
}

type SourcesSinks interface {
	Sinks() []source_sink.MeteredSink
	FlushAndPushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
	Sources() []source_sink.MeteredSourceIntr
	StartWarmup()
}

type BaseExecutionContext struct {
	BaseSrcsSinks
	BaseProcArgs
}

func NewExecutionContext(
	srcs []source_sink.MeteredSourceIntr,
	sinks []source_sink.MeteredSink,
	funcName string,
	curEpoch uint64,
	parNum uint8,
) BaseExecutionContext {
	return BaseExecutionContext{
		BaseProcArgs: NewBaseProcArgs(funcName, curEpoch, parNum),
		BaseSrcsSinks: BaseSrcsSinks{
			srcs:  srcs,
			sinks: sinks,
		},
	}
}

func NewExecutionContextFromComponents(
	srcsSinks BaseSrcsSinks,
	procArgs BaseProcArgs,
) BaseExecutionContext {
	return BaseExecutionContext{
		BaseProcArgs:  procArgs,
		BaseSrcsSinks: srcsSinks,
	}
}

type BaseSrcsSinks struct {
	srcs  []source_sink.MeteredSourceIntr
	sinks []source_sink.MeteredSink
}

func NewBaseSrcsSinks(srcs []source_sink.MeteredSourceIntr, sinks []source_sink.MeteredSink) BaseSrcsSinks {
	return BaseSrcsSinks{
		srcs:  srcs,
		sinks: sinks,
	}
}

func (pa *BaseSrcsSinks) Sources() []source_sink.MeteredSourceIntr {
	return pa.srcs
}

func (pa *BaseSrcsSinks) FlushAndPushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	for _, sink := range pa.sinks {
		err := sink.Produce(ctx, msg, parNum, isControl)
		if err != nil {
			return err
		}
	}
	return nil
}

func (pa *BaseSrcsSinks) Sinks() []source_sink.MeteredSink {
	return pa.sinks
}

func (pa *BaseSrcsSinks) StartWarmup() {
	for _, src := range pa.srcs {
		src.StartWarmup()
	}
	for _, sink := range pa.sinks {
		sink.StartWarmup()
	}
}

type ProcArgs interface {
	SubstreamNum() uint8
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
	CurEpoch(uint64) SetSubstreamNum
}

type SetSubstreamNum interface {
	SubstreamNum(uint8) BuildProcArgs
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
func (b *BaseProcArgsBuilder) CurEpoch(curEpoch uint64) SetSubstreamNum {
	b.bp.curEpoch = curEpoch
	return b
}
func (b *BaseProcArgsBuilder) SubstreamNum(parNum uint8) BuildProcArgs {
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

func (pa *BaseProcArgs) SubstreamNum() uint8 {
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

type ProcessAndReturnFunc func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error)
