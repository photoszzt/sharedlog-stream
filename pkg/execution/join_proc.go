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

func (jm *JoinProcManager) joinProcLoop(
	ctx context.Context,
	task *stream_task.StreamTask,
	procArgs *JoinProcArgs,
	wg *sync.WaitGroup,
) {
	id := ctx.Value(commtypes.CTXID{}).(string)
	// joinProcFlushSt := stats.NewInt64Collector(fmt.Sprintf("%s_flushForALO", id), stats.DEFAULT_COLLECT_DURATION)
	defer wg.Done()
	// debug.Fprintf(os.Stderr, "[id=%s, ts=%d] joinProc start running\n",
	// 	id, time.Now().UnixMilli())
	// resume_us := stats.NewInt64Collector(fmt.Sprintf("joinProc_%s_us", id), stats.DEFAULT_COLLECT_DURATION)
	for {
		select {
		case <-ctx.Done():
			return
		case <-jm.done:
			// debug.Fprintf(os.Stderr, "[id=%s, ts=%d] got done msg\n",
			// 	id, time.Now().UnixMilli())
			return
		default:
		}
		jm.runLock.Lock()
		// debug.Fprintf(os.Stderr, "[id=%s] before consume, stream name %s\n", id, procArgs.Consumers()[0].Stream().TopicName())
		gotMsgs, err := procArgs.Consumers()[0].Consume(ctx, procArgs.SubstreamNum())
		if err != nil {
			if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
				// debug.Fprintf(os.Stderr, "[TIMEOUT] %s %s timeout, out chan len: %d\n",
				// 	id, procArgs.Consumers()[0].TopicName(), len(jm.out))
				// out <- &common.FnOutput{Success: true, Message: err.Error()}
				// debug.Fprintf(os.Stderr, "%s done sending msg\n", id)
				// return
				jm.runLock.Unlock()
				continue
			}
			fmt.Fprintf(os.Stderr, "[ERROR] consume: %v, out chan len: %d\n", err, len(jm.out))
			jm.out <- &common.FnOutput{Success: false, Message: err.Error()}
			fmt.Fprintf(os.Stderr, "%s done sending msg2\n", id)
			jm.runLock.Unlock()
			return
		}
		// debug.Fprintf(os.Stderr, "[id=%s] after consume\n", id)
		for _, msg := range gotMsgs.Msgs {
			if msg.MsgArr == nil && msg.Msg.Value == nil {
				continue
			}
			if msg.IsControl {
				ret_err := HandleScaleEpochAndBytes(ctx, msg, procArgs)
				if ret_err != nil {
					fmt.Fprintf(os.Stderr, "[SCALE_EPOCH] out: %v, out chan len: %d\n", ret_err, len(jm.out))
					jm.out <- ret_err
					jm.runLock.Unlock()
					return
				}
				continue
			}
			procArgs.Consumers()[0].RecordCurrentConsumedSeqNum(msg.LogSeqNum)

			if msg.MsgArr != nil {
				// debug.Fprintf(os.Stderr, "[id=%s] got msgarr\n", id)
				for _, subMsg := range msg.MsgArr {
					if subMsg.Value == nil {
						continue
					}
					// debug.Fprintf(os.Stderr, "[id=%s] before proc msg with sink1\n", id)
					err = procMsgWithSink(ctx, subMsg, procArgs, id)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink: %v, out chan len: %d\n", id, err, len(jm.out))
						jm.out <- &common.FnOutput{Success: false, Message: err.Error()}
						fmt.Fprintf(os.Stderr, "%s done send msg3\n", id)
						jm.runLock.Unlock()
						return
					}
				}
			} else {
				// debug.Fprintf(os.Stderr, "[id=%s] got single msg\n", id)
				if msg.Msg.Value == nil {
					continue
				}
				err = procMsgWithSink(ctx, msg.Msg, procArgs, id)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink2: %v, out chan len: %d\n", id, err, len(jm.out))
					jm.out <- &common.FnOutput{Success: false, Message: err.Error()}
					fmt.Fprintf(os.Stderr, "[id=%s] done send msg4\n", id)
					jm.runLock.Unlock()
					return
				}
			}
		}
		jm.runLock.Unlock()
		// debug.Fprintf(os.Stderr, "[id=%s] after for loop\n", id)
	}
}

func procMsgWithSink(ctx context.Context, msg commtypes.Message, procArgs *JoinProcArgs, id string,
) error {
	// st := msg.Value.(commtypes.EventTimeExtractor)
	// ts, err := st.ExtractEventTime()
	// if err != nil {
	// 	debug.Fprintf(os.Stderr, "[ERROR] %s return extract ts: %v\n", ctx.Value("id"), err)
	// 	return fmt.Errorf("fail to extract timestamp: %v", err)
	// }
	// msg.Timestamp = ts

	// debug.Fprintf(os.Stderr, "[id=%s] before runner\n", id)
	msgs, err := procArgs.runner(ctx, msg)
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return runner: %v\n", ctx.Value("id"), err)
		return err
	}
	// debug.Fprintf(os.Stderr, "[id=%s] after runner\n", id)
	for _, msg := range msgs {
		// debug.Fprintf(os.Stderr, "k %v, v %v, ts %d\n", msg.Key, msg.Value, msg.Timestamp)
		err = procArgs.Producers()[0].Produce(ctx, msg, procArgs.SubstreamNum(), false)
		if err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] %s return push to sink: %v\n", ctx.Value("id"), err)
			return err
		}
	}
	// debug.Fprintf(os.Stderr, "[id=%s] after produce\n", id)
	return nil
}
