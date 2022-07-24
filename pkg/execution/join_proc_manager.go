package execution

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/utils/syncutils"
	"sync"
)

type JoinWorkerFunc func(c context.Context, m commtypes.Message) ([]commtypes.Message, error)

type JoinProcManager struct {
	runLock syncutils.Mutex
	out     chan *common.FnOutput
	done    chan struct{}
}

func NewJoinProcManager() *JoinProcManager {
	out := make(chan *common.FnOutput, 1)
	return &JoinProcManager{
		out: out,
	}
}

func (jm *JoinProcManager) Out() <-chan *common.FnOutput {
	return jm.out
}

func (jm *JoinProcManager) LockRunlock() {
	jm.runLock.Lock()
}

func (jm *JoinProcManager) UnlockRunlock() {
	jm.runLock.Unlock()
}

func LaunchJoinProcLoop(
	ctx context.Context,
	jm *JoinProcManager,
	task *stream_task.StreamTask,
	procArgs *JoinProcArgs,
	wg *sync.WaitGroup,
) {
	jm.done = make(chan struct{})
	wg.Add(1)
	go joinProcLoop(ctx, jm, task, procArgs, wg)
}

func (jm *JoinProcManager) RequestToTerminate() {
	close(jm.done)
}
