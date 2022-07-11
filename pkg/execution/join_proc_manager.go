package execution

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stream_task"
	"sync"
)

type JoinWorkerFunc func(c context.Context, m commtypes.Message) ([]commtypes.Message, error)

type JoinProcManager struct {
	out    chan *common.FnOutput
	done   chan struct{}
	pause  chan struct{}
	resume chan struct{}
}

func NewJoinProcManager() *JoinProcManager {
	out := make(chan *common.FnOutput, 1)
	return &JoinProcManager{
		out:    out,
		pause:  make(chan struct{}),
		resume: make(chan struct{}),
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

func (jm *JoinProcManager) LaunchJoinProcLoop(
	ctx context.Context,
	task *stream_task.StreamTask,
	procArgs *JoinProcArgs,
	wg *sync.WaitGroup,
) {
	jm.done = make(chan struct{})
	wg.Add(1)
	go joinProcLoop(ctx, jm.out, task, procArgs, wg, jm.done, jm.pause, jm.resume)
}

func (jm *JoinProcManager) RequestToTerminate() {
	close(jm.done)
}
