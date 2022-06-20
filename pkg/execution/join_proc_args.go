package execution

import (
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
)

type JoinProcArgs struct {
	runner JoinWorkerFunc
	processor.BaseExecutionContext
}

// var _ = proc_interface.ProcArgsWithSink(&JoinProcArgs{})

func NewJoinProcArgs(
	runner JoinWorkerFunc,
	ectx processor.BaseExecutionContext,
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
	leftArgs := NewJoinProcArgs(runnerL, processor.NewExecutionContextFromComponents(
		proc_interface.NewBaseSrcsSinks(srcs[:1], sinks),
		procArgs,
	))
	rightArgs := NewJoinProcArgs(runnerL, processor.NewExecutionContextFromComponents(
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
