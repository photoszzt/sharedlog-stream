package execution

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/stats"
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
		leftJoinWorker, rightJoinWorker,
		allConsumersProducers.Consumers(),
		allConsumersProducers.Producers(), baseProcArgs)
	var wg sync.WaitGroup
	leftManager := NewJoinProcManager()
	rightManager := NewJoinProcManager()

	procArgs := NewCommonJoinProcArgs(
		joinProcLeft, joinProcRight,
		leftManager.Out(), rightManager.Out(),
		allConsumersProducers)
	lctx := context.WithValue(ctx, commtypes.CTXID{}, "left")
	rctx := context.WithValue(ctx, commtypes.CTXID{}, "right")

	pauseTime := stats.NewInt64Collector("join_pause_us", stats.DEFAULT_COLLECT_DURATION)
	resumeTime := stats.NewInt64Collector("join_resume_us", stats.DEFAULT_COLLECT_DURATION)

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			return HandleJoinErrReturn(argsTmp)
		}).
		InitFunc(func(task *stream_task.StreamTask) {
			// debug.Fprintf(os.Stderr, "init ts=%d launch join proc loops\n", time.Now().UnixMilli())
			leftManager.LaunchJoinProcLoop(lctx, task, joinProcLeft, &wg)
			rightManager.LaunchJoinProcLoop(rctx, task, joinProcRight, &wg)
			// debug.Fprintf(os.Stderr, "init ts=%d done invoke join proc loops\n", time.Now().UnixMilli())
		}).
		PauseFunc(func(sargs *stream_task.StreamTaskArgs) *common.FnOutput {
			// debug.Fprintf(os.Stderr, "in pause func\n")
			if ret := HandleJoinErrReturn(procArgs); ret != nil {
				return ret
			}
			pStart := stats.TimerBegin()
			leftManager.LockRunlock()
			rightManager.LockRunlock()
			elapsed := stats.Elapsed(pStart)
			pauseTime.AddSample(elapsed.Microseconds())
			return nil
		}).
		ResumeFunc(func(task *stream_task.StreamTask, sargs *stream_task.StreamTaskArgs) {
			// debug.Fprintf(os.Stderr, "resume join porc\n")
			rStart := stats.TimerBegin()
			leftManager.UnlockRunlock()
			rightManager.UnlockRunlock()
			elapsed := stats.Elapsed(rStart)
			resumeTime.AddSample(elapsed.Microseconds())
			// debug.Fprintf(os.Stderr, "done resume join proc\n")
		}).Build()
	return task, procArgs
}
