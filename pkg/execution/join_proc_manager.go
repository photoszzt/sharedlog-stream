package execution

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stream_task"
	"sync"
)

type JoinWorker struct {
	joinWorkerFunc JoinWorkerFunc
	flushTableFunc func(ctx context.Context) error
}

func NewJoinWorkerWithRunner(runner JoinWorkerFunc) JoinWorker {
	return JoinWorker{
		joinWorkerFunc: runner,
		flushTableFunc: func(ctx context.Context) error { return nil },
	}
}

func NewJoinWorker(runner JoinWorkerFunc, flushTableFunc FlushFunc) JoinWorker {
	return JoinWorker{
		joinWorkerFunc: runner,
		flushTableFunc: flushTableFunc,
	}
}

type JoinWorkerFunc func(c context.Context, m commtypes.Message) ([]commtypes.Message, error)
type FlushFunc func(ctx context.Context) error

type JoinProcManager struct {
	runLock     syncutils.Mutex
	out         chan *common.FnOutput
	done        chan struct{}
	pause       chan struct{}
	resume      chan struct{}
	shouldFlush uint64
}

func NewJoinProcManager() *JoinProcManager {
	out := make(chan *common.FnOutput, 1)
	return &JoinProcManager{
		out:         out,
		pause:       make(chan struct{}),
		resume:      make(chan struct{}),
		shouldFlush: make(chan struct{}),
	}
}

func (jm *JoinProcManager) Out() <-chan *common.FnOutput {
	return jm.out
}

func (jm *JoinProcManager) Resume() chan struct{} {
	return jm.resume
}

func (jm *JoinProcManager) Pause() chan struct{} {
	return jm.pause
}

func (jm *JoinProcManager) ShouldFlush() chan struct{} {
	return jm.shouldFlush
}

func (jm *JoinProcManager) LaunchJoinProcLoop(
	ctx context.Context,
	task *stream_task.StreamTask,
	procArgs *JoinProcArgs,
	wg *sync.WaitGroup,
) {
	jm.done = make(chan struct{})
	wg.Add(1)
	go jm.joinProcLoop(ctx, task, procArgs, wg)
}

func (jm *JoinProcManager) RequestToTerminate() {
	close(jm.done)
}
