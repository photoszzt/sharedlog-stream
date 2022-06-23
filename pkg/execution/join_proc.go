package execution

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream_task"
	"sync"

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
	id := ctx.Value(commtypes.CTXID("id")).(string)
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
		gotMsgs, err := procArgs.Consumers()[0].Consume(ctx, procArgs.SubstreamNum())
		if err != nil {
			if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
				debug.Fprintf(os.Stderr, "[TIMEOUT] %s %s timeout, out chan len: %d\n",
					id, procArgs.Consumers()[0].TopicName(), len(out))
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
			task.CurrentConsumeOffset[procArgs.Consumers()[0].TopicName()] = msg.LogSeqNum
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
		err = procArgs.Producers()[0].Produce(ctx, msg, procArgs.SubstreamNum(), false)
		if err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] %s return push to sink: %v\n", ctx.Value("id"), err)
			return err
		}
	}
	return nil
}
