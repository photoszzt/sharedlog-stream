package execution

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/txn_data"

	"golang.org/x/xerrors"
)

func CommonProcess(ctx context.Context, t *stream_task.StreamTask, ectx *processor.BaseExecutionContext,
	procMsg proc_interface.ProcessMsgFunc, gotEndMark *bool,
) *common.FnOutput {
	if t.HandleErrFunc != nil {
		if err := t.HandleErrFunc(); err != nil {
			return common.GenErrFnOutput(err)
		}
	}
	consumer := ectx.Consumers()[0]
	isInitialSrc := consumer.IsInitialSource()
	gotMsgs, err := consumer.Consume(ctx, ectx.SubstreamNum())
	if err != nil {
		// debug.Fprintf(os.Stderr, "consume %s %d error %v\n",
		// 	consumer.TopicName(), ectx.SubstreamNum(), err)
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			// count := consumer.GetCount()
			// debug.Fprintf(os.Stderr, "consume %s %d timeout, count %d\n",
			// 	consumer.TopicName(), ectx.SubstreamNum(), count)
			return &common.FnOutput{Success: true, Message: err.Error()}
		}
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	msgs := gotMsgs.Msgs
	if msgs.MsgArr == nil && msgs.Msg.Key == nil && msgs.Msg.Value == nil {
		return nil
	}
	if msgs.IsControl {
		key := msgs.Msg.Key.(string)
		if key == txn_data.SCALE_FENCE_KEY {
			ret_err := HandleScaleEpochAndBytes(ctx, msgs, ectx)
			if ret_err != nil {
				return ret_err
			}
			ectx.Consumers()[0].RecordCurrentConsumedSeqNum(msgs.LogSeqNum)
			return nil
		} else if key == commtypes.END_OF_STREAM_KEY {
			v := msgs.Msg.Value.(producer_consumer.StartTimeAndBytes)
			msgToPush := commtypes.Message{Key: msgs.Msg.Key, Value: v.EpochMarkEncoded}
			for _, sink := range ectx.Producers() {
				if sink.Stream().NumPartition() > ectx.SubstreamNum() {
					// debug.Fprintf(os.Stderr, "produce stream end mark to %s %d\n",
					// 	sink.Stream().TopicName(), ectx.SubstreamNum())
					err := sink.Produce(ctx, msgToPush, ectx.SubstreamNum(), true)
					if err != nil {
						return &common.FnOutput{Success: false, Message: err.Error()}
					}
				}
			}
			t.SetEndDuration(v.StartTime)
			*gotEndMark = true
			return nil
		} else {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("unrecognized key: %v", key)}
		}
	} else {
		// err = proc(t, msg)
		ectx.Consumers()[0].RecordCurrentConsumedSeqNum(msgs.LogSeqNum)
		err = ProcessMsgAndSeq(ctx, msgs, ectx, procMsg, isInitialSrc)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		return nil
	}
}

func extractEventTs(msg *commtypes.Message, isInitialSrc bool) error {
	if isInitialSrc {
		err := msg.ExtractEventTimeFromVal()
		// debug.Fprintf(os.Stderr, "msg k %v, v %v, ts %d\n", msg.Key, msg.Value, msg.Timestamp)
		return err
	}
	// debug.Fprintf(os.Stderr, "msg k %v, v %v, ts %d\n", msg.Key, msg.Value, msg.Timestamp)
	return nil
}

func ProcessMsgAndSeq(ctx context.Context, msg *commtypes.MsgAndSeq, args processor.ExecutionContext,
	procMsg proc_interface.ProcessMsgFunc, isInitialSrc bool,
) error {
	if msg.MsgArr != nil {
		for _, subMsg := range msg.MsgArr {
			if subMsg.Key == nil && subMsg.Value == nil {
				continue
			}
			meteredConsumer := args.Consumers()[0]
			meteredConsumer.ExtractProduceToConsumeTime(&subMsg)
			err := extractEventTs(&subMsg, isInitialSrc)
			if err != nil {
				return err
			}
			err = procMsg(ctx, subMsg, args)
			if err != nil {
				return err
			}
		}
		return nil
	}
	err := extractEventTs(&msg.Msg, isInitialSrc)
	if err != nil {
		return err
	}
	args.Consumers()[0].ExtractProduceToConsumeTime(&msg.Msg)
	return procMsg(ctx, msg.Msg, args)
}

func HandleScaleEpochAndBytes(ctx context.Context, msg *commtypes.MsgAndSeq,
	args processor.ExecutionContext,
) *common.FnOutput {
	v := msg.Msg.Value.(producer_consumer.ScaleEpochAndBytes)
	msgToPush := commtypes.Message{Key: msg.Msg.Key, Value: v.Payload}
	for _, sink := range args.Producers() {
		if sink.Stream().NumPartition() > args.SubstreamNum() {
			err := sink.Produce(ctx, msgToPush, args.SubstreamNum(), true)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
	}
	if args.CurEpoch() < v.ScaleEpoch {
		err := args.RecordFinishFunc()(ctx, args.FuncName(), args.SubstreamNum())
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		return &common.FnOutput{
			Success: true,
			Message: fmt.Sprintf("%s-%d epoch %d exit", args.FuncName(), args.SubstreamNum(), args.CurEpoch()),
			Err:     common_errors.ErrShouldExitForScale,
		}
	}
	return nil
}
