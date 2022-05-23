package execution

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sync"

	"golang.org/x/xerrors"
)

func joinProcLoop(
	ctx context.Context,
	out chan *common.FnOutput,
	task *transaction.StreamTask,
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
		debug.Fprintf(os.Stderr, "before consume\n")
		gotMsgs, err := procArgs.src.Consume(ctx, procArgs.ParNum())
		if err != nil {
			if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
				debug.Fprintf(os.Stderr, "[TIMEOUT] %s %s timeout, out chan len: %d\n",
					id, procArgs.src.TopicName(), len(out))
				out <- &common.FnOutput{Success: true, Message: err.Error()}
				debug.Fprintf(os.Stderr, "%s done sending msg\n", id)
				return
			}
			debug.Fprintf(os.Stderr, "[ERROR] consume: %v, out chan len: %d\n", err, len(out))
			out <- &common.FnOutput{Success: false, Message: err.Error()}
			debug.Fprintf(os.Stderr, "%s done sending msg2\n", id)
			return
		}
		debug.Fprintf(os.Stderr, "after consume\n")
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
			task.CurrentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
			task.OffMu.Unlock()

			if msg.MsgArr != nil {
				debug.Fprintf(os.Stderr, "got msgarr\n")
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
				debug.Fprintf(os.Stderr, "got single msg\n")
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
		debug.Fprintf(os.Stderr, "after for loop\n")
	}
}

func procMsgWithSink(ctx context.Context, msg commtypes.Message, procArgs *JoinProcArgs,
) error {
	st := msg.Value.(commtypes.StreamTimeExtractor)
	ts, err := st.ExtractStreamTime()
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
	err = pushMsgsToSink(ctx, procArgs.Sinks()[0], procArgs.cHash, procArgs.cHashMu, msgs, procArgs.trackParFunc)
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return push to sink: %v\n", ctx.Value("id"), err)
		return err
	}
	return nil
}

func pushMsgsToSink(
	ctx context.Context,
	sink source_sink.Sink,
	cHash *hash.ConsistentHash,
	cHashMu *sync.RWMutex,
	msgs []commtypes.Message,
	trackParFunc tran_interface.TrackKeySubStreamFunc,
) error {
	for _, msg := range msgs {
		key := msg.Key.(uint64)
		cHashMu.RLock()
		parTmp, ok := cHash.Get(key)
		cHashMu.RUnlock()
		if !ok {
			return fmt.Errorf("fail to calculate partition")
		}
		par := parTmp.(uint8)
		err := trackParFunc(ctx, key, sink.KeySerde(), sink.TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = sink.Produce(ctx, msg, par, false)
		if err != nil {
			return err
		}
	}
	return nil
}
