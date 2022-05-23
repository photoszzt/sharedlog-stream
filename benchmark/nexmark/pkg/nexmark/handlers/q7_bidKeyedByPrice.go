package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q7BidKeyedByPrice struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQ7BidKeyedByPriceHandler(env types.Environment, funcName string) types.FuncHandler {
	return &q7BidKeyedByPrice{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *q7BidKeyedByPrice) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.processQ7BidKeyedByPrice(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

type q7BidKeyedByPriceProcessArgs struct {
	src             *source_sink.MeteredSource
	bid             *processor.MeteredProcessor
	bidKeyedByPrice *processor.MeteredProcessor
	output_stream   *sharedlog_stream.ShardedSharedLogStream
	trackParFunc    tran_interface.TrackKeySubStreamFunc
	proc_interface.BaseProcArgsWithSink
}

func (a *q7BidKeyedByPriceProcessArgs) Source() source_sink.Source { return a.src }

func (h *q7BidKeyedByPrice) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	args := argsTmp.(*q7BidKeyedByPriceProcessArgs)
	return execution.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				err := h.procMsg(ctx, subMsg, args)
				if err != nil {
					return err
				}
			}
			return nil
		}
		return h.procMsg(ctx, msg.Msg, args)
	})
}

func (h *q7BidKeyedByPrice) procMsg(ctx context.Context, msg commtypes.Message, args *q7BidKeyedByPriceProcessArgs) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	bidMsg, err := args.bid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filter bid err: %v", err)
	}
	if bidMsg != nil {
		mappedKey, err := args.bidKeyedByPrice.ProcessAndReturn(ctx, bidMsg[0])
		if err != nil {
			return fmt.Errorf("bid keyed by price error: %v", err)
		}
		key := mappedKey[0].Key.(uint64)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(key)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to get output substream number")
		}
		par := parTmp.(uint8)
		// par := uint8(key % uint64(args.numOutPartition))
		err = args.trackParFunc(ctx, key, args.Sinks()[0].KeySerde(), args.Sinks()[0].TopicName(), par)
		if err != nil {
			return fmt.Errorf("track par err: %v", err)
		}
		err = args.Sinks()[0].Produce(ctx, mappedKey[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *q7BidKeyedByPrice) processQ7BidKeyedByPrice(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, input)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	src, sink, err := getSrcSinkUint64Key(ctx, input, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src.SetInitialSource(true)

	bid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
		event := msg.Value.(*ntypes.Event)
		return event.Etype == ntypes.BID, nil
	})), time.Duration(input.WarmupS)*time.Second)
	bidKeyedByPrice := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
		event := msg.Value.(*ntypes.Event)
		return commtypes.Message{Key: event.Bid.Price, Value: msg.Value, Timestamp: msg.Timestamp}, nil
	})), time.Duration(input.WarmupS)*time.Second)

	procArgs := &q7BidKeyedByPriceProcessArgs{
		src:             src,
		bid:             bid,
		bidKeyedByPrice: bidKeyedByPrice,
		output_stream:   output_streams[0],
		trackParFunc:    tran_interface.DefaultTrackSubstreamFunc,
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink([]source_sink.Sink{sink}, h.funcName,
			input.ScaleEpoch, input.ParNum),
	}

	task := transaction.StreamTask{
		ProcessFunc:               h.process,
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
	}

	control_channel.SetupConsistentHash(&h.cHashMu, h.cHash, input.NumOutPartitions[0])

	srcs := []source_sink.Source{src}
	sinks_arr := []source_sink.Sink{sink}
	if input.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, input.InputTopicNames[0], input.ParNum, input.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks_arr)
		benchutil.UpdateStreamTaskArgsTransaction(input, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc, recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
				procArgs.(*q7BidKeyedByPriceProcessArgs).trackParFunc = trackParFunc
				procArgs.(*q7BidKeyedByPriceProcessArgs).SetRecordFinishFunc(recordFinishFunc)
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["bid"] = bid.GetLatency()
			ret.Latencies["bidKeyedByPrice"] = bidKeyedByPrice.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr)
	benchutil.UpdateStreamTaskArgs(input, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["bid"] = bid.GetLatency()
		ret.Latencies["bidKeyedByPrice"] = bidKeyedByPrice.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
