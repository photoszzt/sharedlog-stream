package execution

import (
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/source_sink"
)

type JoinProcArgs struct {
	runner JoinWorkerFunc
	proc_interface.BaseExecutionContext
}

// var _ = proc_interface.ProcArgsWithSink(&JoinProcArgs{})

func NewJoinProcArgs(
	src source_sink.Source,
	sink source_sink.Sink,
	runner JoinWorkerFunc,
	funcName string,
	curEpoch uint64,
	parNum uint8,
) *JoinProcArgs {
	return &JoinProcArgs{
		runner: runner,
		BaseExecutionContext: proc_interface.NewExecutionContext(
			[]source_sink.Source{src},
			[]source_sink.Sink{sink}, funcName, curEpoch, parNum),
	}
}

type JoinProcWithoutSinkArgs struct {
	src    source_sink.Source
	runner JoinWorkerFunc
	parNum uint8
}

func NewJoinProcWithoutSinkArgs(src source_sink.Source, runner JoinWorkerFunc, parNum uint8) *JoinProcWithoutSinkArgs {
	return &JoinProcWithoutSinkArgs{
		src:    src,
		runner: runner,
		parNum: parNum,
	}
}
