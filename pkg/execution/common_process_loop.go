package execution

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/txn_data"

	"golang.org/x/xerrors"
)

func CommonProcess(ctx context.Context, t *stream_task.StreamTask, args proc_interface.ExecutionContext,
	procMsg proc_interface.ProcessMsgFunc,
) *common.FnOutput {
	if t.HandleErrFunc != nil {
		if err := t.HandleErrFunc(); err != nil {
			return &common.FnOutput{Success: true, Message: err.Error()}
		}
	}
	gotMsgs, err := args.Sources()[0].Consume(ctx, args.ParNum())
	if err != nil {
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return &common.FnOutput{Success: true, Message: err.Error()}
		}
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr == nil && msg.Msg.Value == nil {
			continue
		}
		if msg.IsControl {
			ret_err := HandleScaleEpochAndBytes(ctx, msg, args)
			if ret_err != nil {
				return ret_err
			}
			continue
		}
		// err = proc(t, msg)
		t.CurrentOffset[args.Sources()[0].TopicName()] = msg.LogSeqNum
		err = ProcessMsgAndSeq(ctx, msg, args, procMsg)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return nil
}

func extractEventTs(msg *commtypes.Message) error {
	event := msg.Value.(commtypes.EventTimeExtractor)
	ts, err := event.ExtractEventTime()
	if err != nil {
		return err
	}
	msg.Timestamp = ts
	return nil
}

func ProcessMsgAndSeq(ctx context.Context, msg commtypes.MsgAndSeq, args interface{},
	procMsg proc_interface.ProcessMsgFunc,
) error {
	if msg.MsgArr != nil {
		for _, subMsg := range msg.MsgArr {
			if subMsg.Value == nil {
				continue
			}
			err := extractEventTs(&subMsg)
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
	err := extractEventTs(&msg.Msg)
	if err != nil {
		return err
	}
	return procMsg(ctx, msg.Msg, args)
}

func HandleScaleEpochAndBytes(ctx context.Context, msg commtypes.MsgAndSeq,
	args proc_interface.ExecutionContext,
) *common.FnOutput {
	v := msg.Msg.Value.(source_sink.ScaleEpochAndBytes)
	err := args.FlushAndPushToAllSinks(ctx, commtypes.Message{Key: txn_data.SCALE_FENCE_KEY,
		Value: v.Payload}, args.ParNum(), true)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	if args.CurEpoch() < v.ScaleEpoch {
		err = args.RecordFinishFunc()(ctx, args.FuncName(), args.ParNum())
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		return &common.FnOutput{
			Success: true,
			Message: fmt.Sprintf("%s-%d epoch %d exit", args.FuncName(), args.ParNum(), args.CurEpoch()),
			Err:     common_errors.ErrShouldExitForScale,
		}
	}
	return nil
}
