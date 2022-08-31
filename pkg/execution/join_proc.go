package execution

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/stream_task"
	"sync"
	"time"

	"golang.org/x/xerrors"
)

func joinProcLoop[KIn, VIn, KOut, VOut any](
	ctx context.Context,
	jm *JoinProcManager,
	task *stream_task.StreamTask,
	procArgs *JoinProcArgs[KIn, VIn, KOut, VOut],
	wg *sync.WaitGroup,
	msgSerdePair MsgSerdePair[KIn, VIn, KOut, VOut],
) {
	id := ctx.Value(commtypes.CTXID{}).(string)
	defer wg.Done()
	// debug.Fprintf(os.Stderr, "[id=%s, ts=%d] joinProc start running\n",
	// 	id, time.Now().UnixMilli())
	lockAcqTime := stats.NewStatsCollector[int64](fmt.Sprintf("%s_lockAcq", id), stats.DEFAULT_COLLECT_DURATION)
	jWStart := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-jm.done:
			// debug.Fprintf(os.Stderr, "[id=%s, ts=%d] got done msg\n",
			// 	id, time.Now().UnixMilli())
			return
		case <-jm.flushAndCollect:
			for _, sink := range procArgs.Producers() {
				if err := sink.Flush(ctx); err != nil {
					jm.out <- &common.FnOutput{Success: false, Message: err.Error()}
					return
				}
			}
		default:
		}
		lSt := stats.TimerBegin()
		jm.runLock.Lock()
		lelapsed := stats.Elapsed(lSt).Microseconds()
		lockAcqTime.AddSample(lelapsed)
		// debug.Fprintf(os.Stderr, "[id=%s] before consume, stream name %s\n", id, procArgs.Consumers()[0].Stream().TopicName())
		consumer := procArgs.Consumers()[0]
		rawMsgSeq, err := consumer.Consume(ctx, procArgs.SubstreamNum())
		if err != nil {
			if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
				// debug.Fprintf(os.Stderr, "[TIMEOUT] %s %s timeout, out chan len: %d\n",
				// 	id, procArgs.Consumers()[0].TopicName(), len(jm.out))
				// out <- &common.FnOutput{Success: true, Message: err.Error()}
				// debug.Fprintf(os.Stderr, "%s done sending msg\n", id)
				// return
				jm.runLock.Unlock()
				// count := consumer.GetCount()
				// debug.Fprintf(os.Stderr, "consume %s %d timeout, count %d\n",
				// 	consumer.TopicName(), procArgs.SubstreamNum(), count)
				continue
			}
			fmt.Fprintf(os.Stderr, "[ERROR] consume: %v, out chan len: %d\n", err, len(jm.out))
			jm.out <- &common.FnOutput{Success: false, Message: err.Error()}
			fmt.Fprintf(os.Stderr, "%s done sending msg2\n", id)
			jm.runLock.Unlock()
			return
		}
		if rawMsgSeq.IsControl {
			if rawMsgSeq.Mark == commtypes.SCALE_FENCE {
				if procArgs.CurEpoch() < rawMsgSeq.ScaleEpoch {
					procArgs.Consumers()[0].RecordCurrentConsumedSeqNum(rawMsgSeq.LogSeqNum)
					jm.gotScaleFence.Set(true)
					jm.ctrlMsg = optional.Some(rawMsgSeq)
					jm.runLock.Unlock()
					return
				} else {
					jm.runLock.Unlock()
					continue
				}
			} else if rawMsgSeq.Mark == commtypes.STREAM_END {
				consumer.SrcProducerEnd(rawMsgSeq.ProdIdx)
				elapsed := time.Since(jWStart)
				fmt.Fprintf(os.Stderr, "[id=%s] joinProc done, elapsed %v\n", id, elapsed)
				if consumer.AllProducerEnded() {
					jm.gotEndMark.Set(true)
					// fmt.Fprintf(os.Stderr, "[id=%s] %s %d ends, start time: %d\n",
					// 	id, consumer.TopicName(), procArgs.SubstreamNum(), jm.startTimeMs)
					jm.ctrlMsg = optional.Some(rawMsgSeq)
					jm.runLock.Unlock()
					return
				} else {
					jm.runLock.Unlock()
					continue
				}
			} else {
				jm.out <- &common.FnOutput{Success: false,
					Message: fmt.Sprintf("unrecognized mark: %v", rawMsgSeq.Mark)}
				jm.runLock.Unlock()
				return
			}
		}
		msgs, err := commtypes.DecodeRawMsgSeqG(rawMsgSeq, msgSerdePair.inMsgSerde)
		if err != nil {
			jm.out <- &common.FnOutput{Success: false, Message: err.Error()}
			jm.runLock.Unlock()
			return
		}
		if msgs.MsgArr == nil && msgs.Msg.Value.IsNone() && msgs.Msg.Key.IsNone() {
			jm.runLock.Unlock()
			continue
		}
		consumer.RecordCurrentConsumedSeqNum(rawMsgSeq.LogSeqNum)
		// debug.Fprintf(os.Stderr, "[id=%s] after consume\n", id)
		if msgs.MsgArr != nil {
			// debug.Fprintf(os.Stderr, "[id=%s] got msgarr\n", id)
			for _, subMsg := range msgs.MsgArr {
				if subMsg.Key.IsNone() && subMsg.Value.IsNone() {
					continue
				}
				producer_consumer.ExtractProduceToConsumeTimeMsgG(consumer, &subMsg)
				// debug.Fprintf(os.Stderr, "[id=%s] before proc msg with sink1\n", id)
				err = procMsgWithSink(ctx, subMsg, msgSerdePair.outMsgSerde, procArgs, id)
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
			if msgs.Msg.Key.IsNone() && msgs.Msg.Value.IsNone() {
				jm.runLock.Unlock()
				continue
			}
			producer_consumer.ExtractProduceToConsumeTimeMsgG(consumer, &msgs.Msg)
			err = procMsgWithSink(ctx, msgs.Msg, msgSerdePair.outMsgSerde, procArgs, id)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink2: %v, out chan len: %d\n", id, err, len(jm.out))
				jm.out <- &common.FnOutput{Success: false, Message: err.Error()}
				fmt.Fprintf(os.Stderr, "[id=%s] done send msg4\n", id)
				jm.runLock.Unlock()
				return
			}
		}
		jm.runLock.Unlock()
		// debug.Fprintf(os.Stderr, "[id=%s] after for loop\n", id)
	}
}

func procMsgWithSink[KIn, VIn, KOut, VOut any](ctx context.Context,
	msg commtypes.MessageG[KIn, VIn],
	outMsgSerde commtypes.MessageGSerdeG[KOut, VOut],
	procArgs *JoinProcArgs[KIn, VIn, KOut, VOut], id string,
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
		msgSerOp, err := commtypes.MsgGToMsgSer(msg, outMsgSerde.GetKeySerdeG(), outMsgSerde.GetValSerdeG())
		if err != nil {
			return err
		}
		msgSer, ok := msgSerOp.Take()
		if ok {
			err = procArgs.Producers()[0].ProduceData(ctx, msgSer, procArgs.SubstreamNum())
			if err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] %s return push to sink: %v\n", ctx.Value("id"), err)
				return err
			}
		}
	}
	// debug.Fprintf(os.Stderr, "[id=%s] after produce\n", id)
	return nil
}
