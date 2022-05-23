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

type bidKeyedByAuction struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewBidKeyedByAuctionHandler(env types.Environment, funcName string) types.FuncHandler {
	return &bidKeyedByAuction{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *bidKeyedByAuction) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processBidKeyedByAuction(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

type bidKeyedByAuctionProcessArgs struct {
	src           *source_sink.MeteredSource
	filterBid     *processor.MeteredProcessor
	selectKey     *processor.MeteredProcessor
	output_stream *sharedlog_stream.ShardedSharedLogStream
	trackParFunc  tran_interface.TrackKeySubStreamFunc
	proc_interface.BaseProcArgsWithSink
	numOutPartition uint8
}

func (a *bidKeyedByAuctionProcessArgs) Source() source_sink.Source { return a.src }

func (h *bidKeyedByAuction) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	args := argsTmp.(*bidKeyedByAuctionProcessArgs)
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

func (h *bidKeyedByAuction) procMsg(ctx context.Context, msg commtypes.Message, args *bidKeyedByAuctionProcessArgs) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	bidMsg, err := args.filterBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	if bidMsg != nil {
		mappedKey, err := args.selectKey.ProcessAndReturn(ctx, bidMsg[0])
		if err != nil {
			return err
		}
		key := mappedKey[0].Key.(uint64)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(key)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to calculate partition")
		}
		par := parTmp.(uint8)
		// par := uint8(key % uint64(args.numOutPartition))
		err = args.trackParFunc(ctx, key, args.Sinks()[0].KeySerde(), args.Sinks()[0].TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		// fmt.Fprintf(os.Stderr, "out msg ts: %v\n", mappedKey[0].Timestamp)
		err = args.Sinks()[0].Produce(ctx, mappedKey[0], par, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *bidKeyedByAuction) processBidKeyedByAuction(ctx context.Context,
	sp *common.QueryInput,
) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	src, sink, err := getSrcSinkUint64Key(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src.SetInitialSource(true)
	filterBid := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.BID, nil
		})), time.Duration(sp.WarmupS)*time.Second)
	selectKey := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(m commtypes.Message) (commtypes.Message, error) {
			event := m.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.Bid.Auction, Value: m.Value, Timestamp: m.Timestamp}, nil
		})), time.Duration(sp.WarmupS)*time.Second)

	procArgs := &bidKeyedByAuctionProcessArgs{
		src:             src,
		filterBid:       filterBid,
		selectKey:       selectKey,
		output_stream:   output_streams[0],
		numOutPartition: sp.NumOutPartitions[0],
		trackParFunc:    tran_interface.DefaultTrackSubstreamFunc,
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink([]source_sink.Sink{sink}, h.funcName,
			sp.ScaleEpoch, sp.ParNum),
	}

	task := transaction.StreamTask{
		ProcessFunc:               h.process,
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
		PauseFunc:                 nil,
		ResumeFunc:                nil,
		InitFunc: func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			filterBid.StartWarmup()
			selectKey.StartWarmup()
		},
	}

	control_channel.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])
	srcs := []source_sink.Source{src}
	sinks := []source_sink.Sink{sink}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
			sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc, recordFinish tran_interface.RecordPrevInstanceFinishFunc) {
				procArgs.(*bidKeyedByAuctionProcessArgs).trackParFunc = trackParFunc
				procArgs.(*bidKeyedByAuctionProcessArgs).SetRecordFinishFunc(recordFinish)
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterBid"] = filterBid.GetLatency()
			ret.Latencies["selectKey"] = selectKey.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["filterBid"] = filterBid.GetLatency()
		ret.Latencies["selectKey"] = selectKey.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
