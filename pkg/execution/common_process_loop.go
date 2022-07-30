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
	procMsg proc_interface.ProcessMsgFunc,
) *common.FnOutput {
	if t.HandleErrFunc != nil {
		if err := t.HandleErrFunc(); err != nil {
			return common.GenErrFnOutput(err)
		}
	}
	isInitialSrc := ectx.Consumers()[0].IsInitialSource()
	gotMsgs, err := ectx.Consumers()[0].Consume(ctx, ectx.SubstreamNum())
	if err != nil {
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return &common.FnOutput{Success: true, Message: err.Error()}
		}
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr == nil && msg.Msg.Key == nil && msg.Msg.Value == nil {
			continue
		}
		if msg.IsControl {
			ret_err := HandleScaleEpochAndBytes(ctx, msg, ectx)
			if ret_err != nil {
				return ret_err
			}
			continue
		}
		// err = proc(t, msg)
		ectx.Consumers()[0].RecordCurrentConsumedSeqNum(msg.LogSeqNum)
		err = ProcessMsgAndSeq(ctx, msg, ectx, procMsg, isInitialSrc)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return nil
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

func ProcessMsgAndSeq(ctx context.Context, msg commtypes.MsgAndSeq, args processor.ExecutionContext,
	procMsg proc_interface.ProcessMsgFunc, isInitialSrc bool,
) error {
	if msg.MsgArr != nil {
		for _, subMsg := range msg.MsgArr {
			if subMsg.Value == nil {
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

func HandleScaleEpochAndBytes(ctx context.Context, msg commtypes.MsgAndSeq,
	args processor.ExecutionContext,
) *common.FnOutput {
	v := msg.Msg.Value.(producer_consumer.ScaleEpochAndBytes)
	err := args.FlushAndPushToAllSinks(ctx, commtypes.Message{Key: txn_data.SCALE_FENCE_KEY,
		Value: v.Payload}, args.SubstreamNum(), true)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if args.CurEpoch() < v.ScaleEpoch {
		err = args.RecordFinishFunc()(ctx, args.FuncName(), args.SubstreamNum())
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
