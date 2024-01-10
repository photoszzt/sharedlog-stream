package execution

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/utils/syncutils"
	"sync"
	"sync/atomic"
)

type JoinWorkerFunc[KIn, VIn, KOut, VOut any] func(c context.Context, m commtypes.MessageG[KIn, VIn]) ([]commtypes.MessageG[KOut, VOut], error)

type JoinProcManager struct {
	runLock         syncutils.Mutex
	out             chan *common.FnOutput
	done            chan struct{}
	flushAndCollect chan struct{}
	ctrlMsg         *commtypes.RawMsgAndSeq
	gotEndMark      atomic.Bool
	gotScaleFence   atomic.Bool
	startTimeMs     int64
}

func NewJoinProcManager() *JoinProcManager {
	out := make(chan *common.FnOutput, 1)
	p := &JoinProcManager{
		out:             out,
		flushAndCollect: make(chan struct{}),
	}
	p.gotEndMark.Store(false)
	p.gotScaleFence.Store(false)
	return p
}

func (jm *JoinProcManager) Out() <-chan *common.FnOutput {
	return jm.out
}

func (jm *JoinProcManager) GotEndMark() bool {
	return jm.gotEndMark.Load()
}

func (jm *JoinProcManager) GotScaleFence() bool {
	return jm.gotScaleFence.Load()
}

func (jm *JoinProcManager) StreamStartTime() int64 {
	return jm.startTimeMs
}

func (jm *JoinProcManager) LockRunlock() {
	jm.runLock.Lock()
}

func (jm *JoinProcManager) UnlockRunlock() {
	jm.runLock.Unlock()
}

func LaunchJoinProcLoop[KIn, VIn, KOut, VOut any](
	ctx context.Context,
	jm *JoinProcManager,
	task *stream_task.StreamTask,
	procArgs *JoinProcArgs[KIn, VIn, KOut, VOut],
	wg *sync.WaitGroup,
	msgSerdePair MsgSerdePair[KIn, VIn, KOut, VOut],
) {
	jm.done = make(chan struct{})
	wg.Add(1)
	go joinProcLoop(ctx, jm, task, procArgs, wg, msgSerdePair)
}

func (jm *JoinProcManager) RequestToTerminate() {
	close(jm.done)
}
