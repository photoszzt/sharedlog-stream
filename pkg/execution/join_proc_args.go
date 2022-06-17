package execution

import (
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/producer_consumer"
)

type JoinProcArgs struct {
	runner JoinWorkerFunc
	proc_interface.BaseExecutionContext
}

// var _ = proc_interface.ProcArgsWithSink(&JoinProcArgs{})

func NewJoinProcArgs(
	runner JoinWorkerFunc,
	ectx proc_interface.BaseExecutionContext,
) *JoinProcArgs {
	return &JoinProcArgs{
		runner:               runner,
		BaseExecutionContext: ectx,
	}
}

func CreateJoinProcArgsPair(
	runnerL JoinWorkerFunc,
	runnerR JoinWorkerFunc,
	srcs []producer_consumer.MeteredConsumerIntr,
	sinks []producer_consumer.MeteredProducerIntr,
	procArgs proc_interface.BaseProcArgs,
) (*JoinProcArgs, *JoinProcArgs) {
	leftArgs := NewJoinProcArgs(runnerL, proc_interface.NewExecutionContextFromComponents(
		proc_interface.NewBaseSrcsSinks(srcs[:1], sinks),
		procArgs,
	))
	rightArgs := NewJoinProcArgs(runnerL, proc_interface.NewExecutionContextFromComponents(
		proc_interface.NewBaseSrcsSinks(srcs[1:], sinks),
		procArgs,
	))
	return leftArgs, rightArgs
}

type JoinProcWithoutSinkArgs struct {
	src    producer_consumer.Consumer
	runner JoinWorkerFunc
	parNum uint8
}

func NewJoinProcWithoutSinkArgs(src producer_consumer.Consumer, runner JoinWorkerFunc, parNum uint8) *JoinProcWithoutSinkArgs {
	return &JoinProcWithoutSinkArgs{
		src:    src,
		runner: runner,
		parNum: parNum,
	}
}
