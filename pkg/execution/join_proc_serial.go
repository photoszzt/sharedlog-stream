package execution

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/stream_task"

	"golang.org/x/xerrors"
)

type JoinProcManagerSerial struct {
	leftInputChan  chan ConsumeResult
	rightInputChan chan ConsumeResult
}

func NewJoinProcManagerSerial() *JoinProcManagerSerial {
	return &JoinProcManagerSerial{
		leftInputChan:  make(chan ConsumeResult, 1),
		rightInputChan: make(chan ConsumeResult, 1),
	}
}

func (jm *JoinProcManagerSerial) CommonProcessJoin(ctx context.Context,
	t *stream_task.StreamTask, ectx processor.ExecutionContext,
	leftRunner JoinWorkerFunc, rightRunner JoinWorkerFunc,
) *common.FnOutput {
	// left
	subNum := ectx.SubstreamNum()
	go func() {
		gotMsgs, err := ectx.Consumers()[0].Consume(ctx, subNum)
		if err != nil {
			jm.leftInputChan <- ConsumeErr(err)
			return
		}
		jm.leftInputChan <- ConsumeVal(gotMsgs)
	}()
	go func() {
		gotMsgs, err := ectx.Consumers()[1].Consume(ctx, subNum)
		if err != nil {
			jm.rightInputChan <- ConsumeErr(err)
			return
		}
		jm.rightInputChan <- ConsumeVal(gotMsgs)
	}()
	leftInput, rightInput := <-jm.leftInputChan, <-jm.rightInputChan
	if leftInput.Valid() {
		leftMsgSeqs := leftInput.Value()
		ret := processMsgWithRunner(ctx, leftMsgSeqs, ectx, ectx.Consumers()[0], leftRunner)
		if ret != nil {
			return ret
		}
	}
	if rightInput.Valid() {
		rightMsgSeqs := rightInput.Value()
		ret := processMsgWithRunner(ctx, rightMsgSeqs, ectx, ectx.Consumers()[1], rightRunner)
		if ret != nil {
			return ret
		}
	}
	if !leftInput.Valid() {
		err := leftInput.Err()
		if !xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return common.GenErrFnOutput(err)
		}
	}
	if !rightInput.Valid() {
		err := rightInput.Err()
		if !xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return common.GenErrFnOutput(err)
		}
	}
	return nil
}

func processMsgWithRunner(ctx context.Context,
	msgSeqs *commtypes.MsgAndSeqs,
	ectx processor.ExecutionContext,
	consumer producer_consumer.MeteredConsumerIntr,
	runner JoinWorkerFunc,
) *common.FnOutput {
	producer := ectx.Producers()[0]
	substreamNum := ectx.SubstreamNum()
	for _, msg := range msgSeqs.Msgs {
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
		consumer.RecordCurrentConsumedSeqNum(msg.LogSeqNum)
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil && subMsg.Key == nil {
					continue
				}
				consumer.ExtractProduceToConsumeTime(&subMsg)
				err := procMsgWithSink(ctx, subMsg, runner, producer, substreamNum, "")
				if err != nil {
					return common.GenErrFnOutput(err)
				}
			}
		} else {
			if msg.Msg.Value == nil && msg.Msg.Key == nil {
				continue
			}
			consumer.ExtractProduceToConsumeTime(&msg.Msg)
			err := procMsgWithSink(ctx, msg.Msg, runner, producer, substreamNum, "")
			if err != nil {
				return common.GenErrFnOutput(err)
			}
		}
	}
	return nil
}
