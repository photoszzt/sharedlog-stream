package execution

import (
	"context"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/stream_task"
	"sync"
)

func PrepareTaskWithJoin(
	ctx context.Context,
	leftJoinWorker JoinWorkerFunc,
	rightJoinWorker JoinWorkerFunc,
	allConsumersProducers proc_interface.BaseConsumersProducers,
	baseProcArgs proc_interface.BaseProcArgs,
) (*stream_task.StreamTask, *CommonJoinProcArgs) {
	joinProcLeft, joinProcRight := CreateJoinProcArgsPair(
		JoinWorkerFunc(leftJoinWorker),
		JoinWorkerFunc(rightJoinWorker),
		allConsumersProducers.Consumers(),
		allConsumersProducers.Producers(), baseProcArgs)
	var wg sync.WaitGroup
	leftManager := NewJoinProcManager()
	rightManager := NewJoinProcManager()

	procArgs := NewCommonJoinProcArgs(
		joinProcLeft, joinProcRight,
		leftManager.Out(), rightManager.Out(),
		allConsumersProducers)
	lctx := context.WithValue(ctx, commtypes.CTXID("id"), "left")
	rctx := context.WithValue(ctx, commtypes.CTXID("id"), "right")

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			return HandleJoinErrReturn(argsTmp)
		}).
		InitFunc(func(progArgs interface{}) {
			leftManager.Run()
			rightManager.Run()
		}).
		PauseFunc(func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "in flush func\n")
			leftManager.RequestToTerminate()
			rightManager.RequestToTerminate()
			debug.Fprintf(os.Stderr, "waiting join proc to exit\n")
			wg.Wait()
			if ret := HandleJoinErrReturn(procArgs); ret != nil {
				return ret
			}
			// debug.Fprintf(os.Stderr, "join procs exited\n")
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask) {
			// debug.Fprintf(os.Stderr, "resume join porc\n")
			leftManager.LaunchJoinProcLoop(lctx, task, joinProcLeft, &wg)
			rightManager.LaunchJoinProcLoop(rctx, task, joinProcRight, &wg)

			leftManager.Run()
			rightManager.Run()
			// debug.Fprintf(os.Stderr, "done resume join proc\n")
		}).Build()

	leftManager.LaunchJoinProcLoop(lctx, task, joinProcLeft, &wg)
	rightManager.LaunchJoinProcLoop(rctx, task, joinProcRight, &wg)

	return task, procArgs
}
