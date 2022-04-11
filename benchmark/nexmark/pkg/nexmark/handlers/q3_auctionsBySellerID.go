package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type query3AuctionsBySellerIDHandler struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQuery3AuctionsBySellerID(env types.Environment, funcName string) types.FuncHandler {
	return &query3AuctionsBySellerIDHandler{
		env:      env,
		cHash:    hash.NewConsistentHash(),
		funcName: funcName,
	}
}

func (h *query3AuctionsBySellerIDHandler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*AuctionsBySellerIDProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
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

func (h *query3AuctionsBySellerIDHandler) procMsg(ctx context.Context, msg commtypes.Message,
	args *AuctionsBySellerIDProcessArgs,
) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	filteredMsgs, err := args.filterAuctions.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("filterAuctions err: %v", err)
	}
	for _, filteredMsg := range filteredMsgs {
		changeKeyedMsg, err := args.auctionsBySellerIDMap.ProcessAndReturn(ctx, filteredMsg)
		if err != nil {
			return fmt.Errorf("auctionsBySellerIDMap err: %v", err)
		}

		k := changeKeyedMsg[0].Key.(uint64)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(k)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to get output partition")
		}
		par := parTmp.(uint8)
		err = args.trackParFunc(ctx, k, args.sink.KeySerde(), args.sink.TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		fmt.Fprintf(os.Stderr, "append %v to substream %v\n", changeKeyedMsg[0], par)
		err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

type AuctionsBySellerIDProcessArgs struct {
	src                   *processor.MeteredSource
	sink                  *processor.MeteredSink
	filterAuctions        *processor.MeteredProcessor
	auctionsBySellerIDMap *processor.MeteredProcessor
	trackParFunc          transaction.TrackKeySubStreamFunc
	recordFinishFunc      transaction.RecordPrevInstanceFinishFunc
	funcName              string
	curEpoch              uint64
	parNum                uint8
}

func (a *AuctionsBySellerIDProcessArgs) Source() processor.Source { return a.src }
func (a *AuctionsBySellerIDProcessArgs) Sink() processor.Sink     { return a.sink }
func (a *AuctionsBySellerIDProcessArgs) ParNum() uint8            { return a.parNum }
func (a *AuctionsBySellerIDProcessArgs) CurEpoch() uint64         { return a.curEpoch }
func (a *AuctionsBySellerIDProcessArgs) FuncName() string         { return a.funcName }
func (a *AuctionsBySellerIDProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func CommonGetSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *processor.MeteredSink, commtypes.MsgSerde, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: eventSerde,
		MsgSerde:   msgSerde,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, msgSerde, nil
}

func (h *query3AuctionsBySellerIDHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Query3AuctionsBySellerID(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *query3AuctionsBySellerIDHandler) Query3AuctionsBySellerID(
	ctx context.Context,
	sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}

	src, sink, msgSerde, err := CommonGetSrcSink(ctx, sp, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	filterAuctions := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(
		func(m *commtypes.Message) (bool, error) {
			event := m.Value.(*ntypes.Event)
			return event.Etype == ntypes.AUCTION && event.NewAuction.Category == 10, nil
		})))

	auctionsBySellerIDMap := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(
		processor.MapperFunc(func(msg commtypes.Message) (commtypes.Message, error) {
			event := msg.Value.(*ntypes.Event)
			return commtypes.Message{Key: event.NewAuction.Seller, Value: msg.Value, Timestamp: msg.Timestamp}, nil
		})))
	procArgs := &AuctionsBySellerIDProcessArgs{
		src:                   src,
		sink:                  sink,
		filterAuctions:        filterAuctions,
		auctionsBySellerIDMap: auctionsBySellerIDMap,
		parNum:                sp.ParNum,
		trackParFunc:          transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc:      transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:              sp.ScaleEpoch,
		funcName:              h.funcName,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}

	transaction.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartition)

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			MsgSerde:     msgSerde,
			Srcs:         srcs,
			OutputStream: output_stream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName,
				sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicName),
			KVChangelogs:          nil,
			WindowStoreChangelogs: nil,
			FixedOutParNum:        0,
			CHash:                 h.cHash,
			CHashMu:               &h.cHashMu,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc,
				recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*AuctionsBySellerIDProcessArgs).trackParFunc = trackParFunc
				procArgs.(*AuctionsBySellerIDProcessArgs).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
			ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["filterAuctions"] = filterAuctions.GetLatency()
		ret.Latencies["auctionsBySellerIDMap"] = auctionsBySellerIDMap.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
