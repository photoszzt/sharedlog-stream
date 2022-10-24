package stream_task

import (
	"context"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"time"

	"golang.org/x/xerrors"
)

func CommonProcess[K, V any](ctx context.Context, t *StreamTask, ectx *processor.BaseExecutionContext,
	procMsg proc_interface.ProcessMsgFunc[K, V], inMsgSerde commtypes.MessageGSerdeG[K, V],
) (*common.FnOutput, optional.Option[commtypes.RawMsgAndSeq]) {
	if t.HandleErrFunc != nil {
		if err := t.HandleErrFunc(); err != nil {
			return common.GenErrFnOutput(err), optional.None[commtypes.RawMsgAndSeq]()
		}
	}
	consumer := ectx.Consumers()[0]
	isInitialSrc := consumer.IsInitialSource()
	rawMsgSeq, err := consumer.Consume(ctx, ectx.SubstreamNum())
	if err != nil {
		// debug.Fprintf(os.Stderr, "consume %s %d error %v\n",
		// 	consumer.TopicName(), ectx.SubstreamNum(), err)
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			// count := consumer.GetCount()
			// debug.Fprintf(os.Stderr, "consume %s %d timeout, count %d\n",
			// 	consumer.TopicName(), ectx.SubstreamNum(), count)
			return &common.FnOutput{Success: true, Message: err.Error()},
				optional.None[commtypes.RawMsgAndSeq]()
		}
		return &common.FnOutput{Success: false, Message: err.Error()},
			optional.None[commtypes.RawMsgAndSeq]()
	}
	if rawMsgSeq.IsControl {
		if rawMsgSeq.Mark == commtypes.SCALE_FENCE {
			if ectx.CurEpoch() < rawMsgSeq.ScaleEpoch {
				ectx.Consumers()[0].RecordCurrentConsumedSeqNum(rawMsgSeq.LogSeqNum)
				return nil, optional.Some(rawMsgSeq)
			}
			return nil, optional.None[commtypes.RawMsgAndSeq]()
		} else if rawMsgSeq.Mark == commtypes.STREAM_END {
			consumer.SrcProducerEnd(rawMsgSeq.ProdIdx)
			if consumer.AllProducerEnded() {
				return nil, optional.Some(rawMsgSeq)
			} else {
				return nil, optional.None[commtypes.RawMsgAndSeq]()
			}
		} else if rawMsgSeq.Mark == commtypes.EPOCH_END {
			return nil, optional.None[commtypes.RawMsgAndSeq]()
		}
	}
	msgs, err := commtypes.DecodeRawMsgSeqG(rawMsgSeq, inMsgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()},
			optional.None[commtypes.RawMsgAndSeq]()
	}
	if msgs.MsgArr == nil && msgs.Msg.Key.IsNone() && msgs.Msg.Value.IsNone() {
		return nil, optional.None[commtypes.RawMsgAndSeq]()
	}
	ectx.Consumers()[0].RecordCurrentConsumedSeqNum(msgs.LogSeqNum)
	err = processMsgAndSeq(ctx, msgs, ectx, procMsg, isInitialSrc, rawMsgSeq.InjTsMs)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()},
			optional.None[commtypes.RawMsgAndSeq]()
	}
	return nil, optional.None[commtypes.RawMsgAndSeq]()
	// }
}

func processMsgAndSeq[K, V any](ctx context.Context,
	msg commtypes.MsgAndSeqG[K, V],
	args processor.ExecutionContext,
	procMsg proc_interface.ProcessMsgFunc[K, V], isInitialSrc bool,
	beforeInjToStreamTs int64,
) error {
	meteredConsumer := args.Consumers()[0]
	if msg.MsgArr != nil {
		for _, subMsg := range msg.MsgArr {
			if subMsg.Key.IsNone() && subMsg.Value.IsNone() {
				continue
			}
			if !isInitialSrc {
				batchTime := beforeInjToStreamTs - subMsg.InjTMs
				meteredConsumer.CollectBatchTime(batchTime)
			}
			producer_consumer.ExtractProduceToConsumeTimeMsgG(meteredConsumer, &subMsg)
			if isInitialSrc {
				err := subMsg.ExtractEventTimeFromVal()
				if err != nil {
					return err
				}
			}
			subMsg.StartProcTime = time.Now()
			err := procMsg(ctx, subMsg, args)
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		if isInitialSrc {
			err := msg.Msg.ExtractEventTimeFromVal()
			if err != nil {
				return err
			}
		}
		producer_consumer.ExtractProduceToConsumeTimeMsgG(meteredConsumer, &msg.Msg)
		msg.Msg.StartProcTime = time.Now()
		err := procMsg(ctx, msg.Msg, args)

		return err
	}
}
