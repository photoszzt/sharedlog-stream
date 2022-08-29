package execution

import (
	"context"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/stream_task"
	"sync"
)

func PrepareTaskWithJoin[KInL, VInL, KOutL, VOutL, KInR, VInR, KOutR, VOutR any](
	ctx context.Context,
	leftJoinWorker JoinWorkerFunc[KInL, VInL, KOutL, VOutL],
	rightJoinWorker JoinWorkerFunc[KInR, VInR, KOutR, VOutR],
	allConsumersProducers proc_interface.BaseConsumersProducers,
	baseProcArgs proc_interface.BaseProcArgs,
	isFinalStage bool,
	inMsgSerdeLeft commtypes.MessageGSerdeG[KInL, VInL],
	outMsgSerdeLeft commtypes.MessageGSerdeG[KOutL, VOutL],
	inMsgSerdeRight commtypes.MessageGSerdeG[KInR, VInR],
	outMsgSerdeRight commtypes.MessageGSerdeG[KOutR, VOutR],
) (*stream_task.StreamTask, *CommonJoinProcArgs[KInL, VInL, KOutL, VOutL, KInR, VInR, KOutR, VOutR]) {
	joinProcLeft, joinProcRight := CreateJoinProcArgsPair(
		leftJoinWorker, rightJoinWorker,
		allConsumersProducers.Consumers(),
		allConsumersProducers.Producers(), baseProcArgs)
	var wg sync.WaitGroup
	leftManager := NewJoinProcManager()
	rightManager := NewJoinProcManager()

	procArgs := NewCommonJoinProcArgs(
		joinProcLeft, joinProcRight,
		allConsumersProducers)
	lctx := context.WithValue(ctx, commtypes.CTXID{}, "left")
	rctx := context.WithValue(ctx, commtypes.CTXID{}, "right")

	pauseTime := stats.NewStatsCollector[int64]("join_pause_us", stats.DEFAULT_COLLECT_DURATION)
	resumeTime := stats.NewStatsCollector[int64]("join_resume_us", stats.DEFAULT_COLLECT_DURATION)

	handleJoinErrReturn := func() *common.FnOutput {
		var out1 *common.FnOutput
		var out2 *common.FnOutput
		select {
		case out2 = <-rightManager.out:
			debug.Fprintf(os.Stderr, "Got out2 out: %v, per out channel len: %d\n", out2, len(rightManager.out))
		case out1 = <-leftManager.out:
			debug.Fprintf(os.Stderr, "Got out1 out: %v, auc out channel len: %d\n", out1, len(leftManager.out))
		default:
		}
		if out1 != nil || out2 != nil {
			debug.Fprintf(os.Stderr, "out1: %v\n", out1)
			debug.Fprintf(os.Stderr, "out2: %v\n", out2)
		}
		if out2 != nil && !out2.Success {
			return out2
		}
		if out1 != nil && !out1.Success {
			return out1
		}
		return nil
	}

	taskBuilder := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
			if leftManager.GotEndMark() && rightManager.GotEndMark() {
				debug.Fprintf(os.Stderr, "join proc got end mark\n")
				// leftStartTime := leftManager.StreamStartTime()
				// task.SetEndDuration(leftStartTime)
				return nil, leftManager.ctrlMsg
			} else if leftManager.GotScaleFence() && rightManager.GotScaleFence() {
				debug.Fprintf(os.Stderr, "join proc got scale fence\n")
				return nil, leftManager.ctrlMsg
			}
			return handleJoinErrReturn(), optional.None[commtypes.RawMsgAndSeq]()
		}).
		InitFunc(func(task *stream_task.StreamTask) {
			// debug.Fprintf(os.Stderr, "init ts=%d launch join proc loops\n", time.Now().UnixMilli())
			LaunchJoinProcLoop(lctx, leftManager, task, joinProcLeft, &wg, inMsgSerdeLeft, outMsgSerdeLeft)
			LaunchJoinProcLoop(rctx, rightManager, task, joinProcRight, &wg, inMsgSerdeRight, outMsgSerdeRight)
			// debug.Fprintf(os.Stderr, "init ts=%d done invoke join proc loops\n", time.Now().UnixMilli())
		}).
		PauseFunc(func() *common.FnOutput {
			// debug.Fprintf(os.Stderr, "in pause func\n")
			if ret := handleJoinErrReturn(); ret != nil {
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
