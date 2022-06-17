package proc_interface

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/exactly_once_intr"
)

type ExecutionContext interface {
	ProcArgs
	SourcesSinks
}

type SourcesSinks interface {
	Sinks() []producer_consumer.MeteredProducerIntr
	FlushAndPushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error
	Sources() []producer_consumer.MeteredConsumerIntr
	StartWarmup()
}

type BaseExecutionContext struct {
	BaseSrcsSinks
	BaseProcArgs
}

func NewExecutionContext(
	srcs []producer_consumer.MeteredConsumerIntr,
	sinks []producer_consumer.MeteredProducerIntr,
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
	srcs  []producer_consumer.MeteredConsumerIntr
	sinks []producer_consumer.MeteredProducerIntr
}

func NewBaseSrcsSinks(srcs []producer_consumer.MeteredConsumerIntr, sinks []producer_consumer.MeteredProducerIntr) BaseSrcsSinks {
	return BaseSrcsSinks{
		srcs:  srcs,
		sinks: sinks,
	}
}

func (pa *BaseSrcsSinks) Sources() []producer_consumer.MeteredConsumerIntr {
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

func (pa *BaseSrcsSinks) Sinks() []producer_consumer.MeteredProducerIntr {
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
	RecordFinishFunc() exactly_once_intr.RecordPrevInstanceFinishFunc
	SetRecordFinishFunc(recordFinishFunc exactly_once_intr.RecordPrevInstanceFinishFunc)
	TrackParFunc() exactly_once_intr.TrackProdSubStreamFunc
	SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc)
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
	TrackParFunc(exactly_once_intr.TrackProdSubStreamFunc) BuildProcArgs
	RecordFinishFunc(exactly_once_intr.RecordPrevInstanceFinishFunc) BuildProcArgs
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
		b.bp.trackParFunc = exactly_once_intr.DefaultTrackProdSubstreamFunc
	}
	if b.bp.recordFinishFunc == nil {
		b.bp.recordFinishFunc = exactly_once_intr.DefaultRecordPrevInstanceFinishFunc
	}
	return b.bp
}
func (b *BaseProcArgsBuilder) TrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) BuildProcArgs {
	b.bp.trackParFunc = trackParFunc
	return b
}
func (b *BaseProcArgsBuilder) RecordFinishFunc(recordFinishFunc exactly_once_intr.RecordPrevInstanceFinishFunc) BuildProcArgs {
	b.bp.recordFinishFunc = recordFinishFunc
	return b
}

type BaseProcArgs struct {
	recordFinishFunc exactly_once_intr.RecordPrevInstanceFinishFunc
	trackParFunc     exactly_once_intr.TrackProdSubStreamFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func NewBaseProcArgs(funcName string, curEpoch uint64, parNum uint8) BaseProcArgs {
	return BaseProcArgs{
		recordFinishFunc: exactly_once_intr.DefaultRecordPrevInstanceFinishFunc,
		trackParFunc:     exactly_once_intr.DefaultTrackProdSubstreamFunc,
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

func (pa *BaseProcArgs) RecordFinishFunc() exactly_once_intr.RecordPrevInstanceFinishFunc {
	return pa.recordFinishFunc
}

func (pa *BaseProcArgs) SetRecordFinishFunc(recordFinishFunc exactly_once_intr.RecordPrevInstanceFinishFunc) {
	pa.recordFinishFunc = recordFinishFunc
}

func (pa *BaseProcArgs) TrackParFunc() exactly_once_intr.TrackProdSubStreamFunc {
	return pa.trackParFunc
}

func (pa *BaseProcArgs) SetTrackParFunc(trackParFunc exactly_once_intr.TrackProdSubStreamFunc) {
	pa.trackParFunc = trackParFunc
}

type ProcessMsgFunc func(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error

type ProcessAndReturnFunc func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error)
