package execution

import (
	"context"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
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
	isFinalStage bool,
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

	pauseTime := stats.NewStatsCollector[int64]("join_pause_us", stats.DEFAULT_COLLECT_DURATION)
	resumeTime := stats.NewStatsCollector[int64]("join_resume_us", stats.DEFAULT_COLLECT_DURATION)

	taskBuilder := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, *commtypes.MsgAndSeq) {
			if leftManager.GotEndMark() && rightManager.GotEndMark() {
				debug.Fprintf(os.Stderr, "join proc got end mark\n")
				// leftStartTime := leftManager.StreamStartTime()
				// task.SetEndDuration(leftStartTime)
				return nil, leftManager.ctrlMsg
			} else if leftManager.GotScaleFence() && rightManager.GotScaleFence() {
				debug.Fprintf(os.Stderr, "join proc got scale fence\n")
				return nil, leftManager.ctrlMsg
			}
			return HandleJoinErrReturn(argsTmp), nil
		}).
		InitFunc(func(task *stream_task.StreamTask) {
			// debug.Fprintf(os.Stderr, "init ts=%d launch join proc loops\n", time.Now().UnixMilli())
			LaunchJoinProcLoop(lctx, leftManager, task, joinProcLeft, &wg)
			LaunchJoinProcLoop(rctx, rightManager, task, joinProcRight, &wg)
			// debug.Fprintf(os.Stderr, "init ts=%d done invoke join proc loops\n", time.Now().UnixMilli())
		}).
		PauseFunc(func() *common.FnOutput {
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
		ResumeFunc(func(task *stream_task.StreamTask) {
			// debug.Fprintf(os.Stderr, "in resume func\n")
			rStart := stats.TimerBegin()
			leftManager.UnlockRunlock()
			rightManager.UnlockRunlock()
			elapsed := stats.Elapsed(rStart)
			resumeTime.AddSample(elapsed.Microseconds())
			// debug.Fprintf(os.Stderr, "done resume func\n")
		})
	if isFinalStage {
		return taskBuilder.MarkFinalStage().Build(), procArgs
	} else {
		return taskBuilder.Build(), procArgs
	}
}
