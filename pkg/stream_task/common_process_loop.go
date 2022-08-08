package stream_task

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/txn_data"

	"golang.org/x/xerrors"
)

func CommonProcess(ctx context.Context, t *StreamTask, ectx *processor.BaseExecutionContext,
	procMsg proc_interface.ProcessMsgFunc,
) (*common.FnOutput, *commtypes.MsgAndSeq) {
	if t.HandleErrFunc != nil {
		if err := t.HandleErrFunc(); err != nil {
			return common.GenErrFnOutput(err), nil
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
			return &common.FnOutput{Success: true, Message: err.Error()}, nil
		}
		return &common.FnOutput{Success: false, Message: err.Error()}, nil
	}
	msgs := gotMsgs.Msgs
	if msgs.MsgArr == nil && msgs.Msg.Key == nil && msgs.Msg.Value == nil {
		return nil, nil
	}
	if msgs.IsControl {
		key := msgs.Msg.Key.(string)
		if key == txn_data.SCALE_FENCE_KEY {
			v := msgs.Msg.Value.(producer_consumer.ScaleEpochAndBytes)
			if ectx.CurEpoch() < v.ScaleEpoch {
				ectx.Consumers()[0].RecordCurrentConsumedSeqNum(msgs.LogSeqNum)
				return nil, msgs
			}
			return nil, nil
		} else if key == commtypes.END_OF_STREAM_KEY {
			v := msgs.Msg.Value.(producer_consumer.StartTimeAndProdIdx)
			consumer.SrcProducerEnd(v.ProdIdx)
			if consumer.AllProducerEnded() {
				return nil, msgs
			} else {
				return nil, nil
			}
		} else {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("unrecognized key: %v", key)}, nil
		}
	} else {
		// err = proc(t, msg)
		ectx.Consumers()[0].RecordCurrentConsumedSeqNum(msgs.LogSeqNum)
		err = ProcessMsgAndSeq(ctx, msgs, ectx, procMsg, isInitialSrc)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}, nil
		}
		return nil, nil
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
