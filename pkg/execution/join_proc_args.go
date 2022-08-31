package execution

import (
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
)

type JoinProcArgs[KIn, VIn, KOut, VOut any] struct {
	runner JoinWorkerFunc[KIn, VIn, KOut, VOut]
	processor.BaseExecutionContext
	kvc []store.KeyValueStoreOpWithChangelog
	wsc []store.WindowStoreOpWithChangelog
}

// var _ = proc_interface.ProcArgsWithSink(&JoinProcArgs{})

func NewJoinProcArgs[KIn, VIn, KOut, VOut any](
	worker JoinWorkerFunc[KIn, VIn, KOut, VOut],
	ectx processor.BaseExecutionContext,
) *JoinProcArgs[KIn, VIn, KOut, VOut] {
	return &JoinProcArgs[KIn, VIn, KOut, VOut]{
		runner:               worker,
		BaseExecutionContext: ectx,
	}
}

func CreateJoinProcArgsPair[KInL, VInL, KOutL, VOutL, KInR, VInR, KOutR, VOutR any](
	runnerL JoinWorkerFunc[KInL, VInL, KOutL, VOutL],
	runnerR JoinWorkerFunc[KInR, VInR, KOutR, VOutR],
	srcs []*producer_consumer.MeteredConsumer,
	sinks []producer_consumer.MeteredProducerIntr,
	procArgs proc_interface.BaseProcArgs,
) (*JoinProcArgs[KInL, VInL, KOutL, VOutL], *JoinProcArgs[KInR, VInR, KOutR, VOutR]) {
	leftArgs := NewJoinProcArgs(runnerL, processor.NewExecutionContextFromComponents(
		proc_interface.NewBaseSrcsSinks(srcs[:1], sinks),
		procArgs,
	))
	rightArgs := NewJoinProcArgs(runnerR, processor.NewExecutionContextFromComponents(
		proc_interface.NewBaseSrcsSinks(srcs[1:], sinks),
		procArgs,
	))
	return leftArgs, rightArgs
}
