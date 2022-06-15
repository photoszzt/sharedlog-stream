package execution

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sync"
	"time"

	"golang.org/x/xerrors"
)

func joinProcLoop(
	ctx context.Context,
	out chan *common.FnOutput,
	task *stream_task.StreamTask,
	procArgs *JoinProcArgs,
	wg *sync.WaitGroup,
	run chan struct{},
	done chan struct{},
) {
	defer wg.Done()
	<-run
	debug.Fprintf(os.Stderr, "joinProc start running\n")
	id := ctx.Value("id").(string)
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			debug.Fprintf(os.Stderr, "got done msg\n")
			return
		default:
		}
		// debug.Fprintf(os.Stderr, "before consume\n")
		gotMsgs, err := procArgs.Sources()[0].Consume(ctx, procArgs.SubstreamNum())
		if err != nil {
			if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
				debug.Fprintf(os.Stderr, "[TIMEOUT] %s %s timeout, out chan len: %d\n",
					id, procArgs.Sources()[0].TopicName(), len(out))
				// out <- &common.FnOutput{Success: true, Message: err.Error()}
				// debug.Fprintf(os.Stderr, "%s done sending msg\n", id)
				// return
				continue
			}
			debug.Fprintf(os.Stderr, "[ERROR] consume: %v, out chan len: %d\n", err, len(out))
			out <- &common.FnOutput{Success: false, Message: err.Error()}
			debug.Fprintf(os.Stderr, "%s done sending msg2\n", id)
			return
		}
		// debug.Fprintf(os.Stderr, "after consume\n")
		for _, msg := range gotMsgs.Msgs {
			if msg.MsgArr == nil && msg.Msg.Value == nil {
				continue
			}
			if msg.IsControl {
				ret_err := HandleScaleEpochAndBytes(ctx, msg, procArgs)
				if ret_err != nil {
					debug.Fprintf(os.Stderr, "[SCALE_EPOCH] out: %v, out chan len: %d\n", ret_err, len(out))
					out <- ret_err
				}
				continue
			}
			task.OffMu.Lock()
			task.CurrentOffset[procArgs.Sources()[0].TopicName()] = msg.LogSeqNum
			task.OffMu.Unlock()

			if msg.MsgArr != nil {
				// debug.Fprintf(os.Stderr, "got msgarr\n")
				for _, subMsg := range msg.MsgArr {
					if subMsg.Value == nil {
						continue
					}
					debug.Fprintf(os.Stderr, "before proc msg with sink1\n")
					err = procMsgWithSink(ctx, subMsg, procArgs)
					if err != nil {
						debug.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink: %v, out chan len: %d\n", id, err, len(out))
						out <- &common.FnOutput{Success: false, Message: err.Error()}
						debug.Fprintf(os.Stderr, "%s done send msg3\n", id)
						return
					}
				}
			} else {
				// debug.Fprintf(os.Stderr, "got single msg\n")
				if msg.Msg.Value == nil {
					continue
				}
				err = procMsgWithSink(ctx, msg.Msg, procArgs)
				if err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink2: %v, out chan len: %d\n", id, err, len(out))
					out <- &common.FnOutput{Success: false, Message: err.Error()}
					debug.Fprintf(os.Stderr, "%s done send msg4\n", id)
					return
				}
			}
		}
		// debug.Fprintf(os.Stderr, "after for loop\n")
	}
}

func procMsgWithSink(ctx context.Context, msg commtypes.Message, procArgs *JoinProcArgs,
) error {
	st := msg.Value.(commtypes.EventTimeExtractor)
	ts, err := st.ExtractEventTime()
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return extract ts: %v\n", ctx.Value("id"), err)
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	msgs, err := procArgs.runner(ctx, msg)
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return runner: %v\n", ctx.Value("id"), err)
		return err
	}
	for _, msg := range msgs {
		err = procArgs.Sinks()[0].Produce(ctx, msg, procArgs.SubstreamNum(), false)
		if err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] %s return push to sink: %v\n", ctx.Value("id"), err)
			return err
		}
	}
	return nil
}

func SetupStreamStreamJoin(
	mpLeft *store_with_changelog.MaterializeParam,
	mpRight *store_with_changelog.MaterializeParam,
	compare concurrent_skiplist.CompareFunc,
	joiner processor.ValueJoinerWithKeyTsFunc,
	jw *processor.JoinWindows,
	warmup time.Duration,
) (proc_interface.ProcessAndReturnFunc,
	proc_interface.ProcessAndReturnFunc,
	map[string]*processor.MeteredProcessor,
	[]*store_restore.WindowStoreChangelog,
	error,
) {
	toLeftTab, leftTab, err := store_with_changelog.ToInMemWindowTableWithChangelog(
		mpLeft, jw, compare, warmup,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	toRightTab, rightTab, err := store_with_changelog.ToInMemWindowTableWithChangelog(
		mpRight, jw, compare, warmup,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	leftJoinRight, rightJoinLeft := ConfigureStreamStreamJoinProcessor(leftTab, rightTab, joiner, jw, warmup)
	leftJoinRightFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		_, err := toLeftTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		return leftJoinRight.ProcessAndReturn(ctx, msg)
	}
	rightJoinLeftFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		_, err := toRightTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		return rightJoinLeft.ProcessAndReturn(ctx, msg)
	}
	wsc := []*store_restore.WindowStoreChangelog{
		store_restore.NewWindowStoreChangelog(leftTab, mpLeft.ChangelogManager(), nil, mpLeft.KVMsgSerdes(), mpLeft.ParNum()),
		store_restore.NewWindowStoreChangelog(rightTab, mpRight.ChangelogManager(), nil, mpRight.KVMsgSerdes(), mpRight.ParNum()),
	}
	return leftJoinRightFunc, rightJoinLeftFunc,
		map[string]*processor.MeteredProcessor{
			"toLeft":  toLeftTab,
			"toRight": toRightTab,
			"lJoinR":  leftJoinRight,
			"rJoinL":  rightJoinLeft,
		}, wsc, nil
}

func ConfigureStreamStreamJoinProcessor(leftTab store.WindowStore,
	rightTab store.WindowStore,
	joiner processor.ValueJoinerWithKeyTsFunc,
	jw *processor.JoinWindows,
	warmup time.Duration,
) (*processor.MeteredProcessor, *processor.MeteredProcessor) {
	sharedTimeTracker := processor.NewTimeTracker()
	leftJoinRight := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(rightTab, jw, joiner, false, true, sharedTimeTracker),
		warmup,
	)
	rightJoinLeft := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor(leftTab, jw,
			processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker),
		warmup,
	)
	return leftJoinRight, rightJoinLeft
}
