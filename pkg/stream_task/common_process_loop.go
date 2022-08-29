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
	/*
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
	*/
	// err = proc(t, msg)
	ectx.Consumers()[0].RecordCurrentConsumedSeqNum(msgs.LogSeqNum)
	err = ProcessMsgAndSeq(ctx, msgs, ectx, procMsg, isInitialSrc)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()},
			optional.None[commtypes.RawMsgAndSeq]()
	}
	return nil, optional.None[commtypes.RawMsgAndSeq]()
	// }
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

func ProcessMsgAndSeq[K, V any](ctx context.Context, msg commtypes.MsgAndSeqG[K, V], args processor.ExecutionContext,
	procMsg proc_interface.ProcessMsgFunc[K, V], isInitialSrc bool,
) error {
	meteredConsumer := args.Consumers()[0]
	if msg.MsgArr != nil {
		for _, subMsg := range msg.MsgArr {
			if subMsg.Key.IsNone() && subMsg.Value.IsNone() {
				continue
			}
			producer_consumer.ExtractProduceToConsumeTimeMsgG(meteredConsumer, &subMsg)
			if isInitialSrc {
				err := subMsg.ExtractEventTimeFromVal()
				if err != nil {
					return err
				}
			}
			err := procMsg(ctx, subMsg, args)
			if err != nil {
				return err
			}
		}
		return nil
	}
	if isInitialSrc {
		err := msg.Msg.ExtractEventTimeFromVal()
		if err != nil {
			return err
		}
	}
	producer_consumer.ExtractProduceToConsumeTimeMsgG(meteredConsumer, &msg.Msg)
	return procMsg(ctx, msg.Msg, args)
}
