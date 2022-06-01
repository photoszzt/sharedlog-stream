package execution

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
)

type JoinWorkerFunc func(c context.Context, m commtypes.Message) ([]commtypes.Message, error)

type JoinProcManager struct {
	out  chan *common.FnOutput
	run  chan struct{}
	done chan struct{}
}

func NewJoinProcManager() *JoinProcManager {
	out := make(chan *common.FnOutput, 1)
	run := make(chan struct{})
	return &JoinProcManager{
		out: out,
		run: run,
	}
}

func (jm *JoinProcManager) Out() <-chan *common.FnOutput {
	return jm.out
}

func (jm *JoinProcManager) LaunchJoinProcLoop(
	ctx context.Context,
	task *transaction.StreamTask,
	procArgs *JoinProcArgs,
	wg *sync.WaitGroup,
) {
	done := make(chan struct{})
	jm.done = done
	wg.Add(1)
	go joinProcLoop(ctx, jm.out, task, procArgs, wg, jm.run, done)
}

func (jm *JoinProcManager) Run() {
	jm.run <- struct{}{}
}

func (jm *JoinProcManager) RequestToTerminate() {
	close(jm.done)
}
