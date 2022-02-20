package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5AuctionBids struct {
	env           types.Environment
	cHashMu       sync.RWMutex
	cHash         *hash.ConsistentHash
	currentOffset map[string]uint64
}

func NewQ5AuctionBids(env types.Environment) *q5AuctionBids {
	return &q5AuctionBids{
		env:           env,
		cHash:         hash.NewConsistentHash(),
		currentOffset: make(map[string]uint64),
	}
}

func (h *q5AuctionBids) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.processQ5AuctionBids(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5AuctionBids) getSrcSink(ctx context.Context, sp *common.QueryInput,
	msgSerde commtypes.MsgSerde, input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *processor.MeteredSink, error) {
	var seSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:   msgSerde,
		KeySerde:   seSerde,
		ValueSerde: aucIdCountSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

func (h *q5AuctionBids) getCountAggProc(sp *common.QueryInput, msgSerde commtypes.MsgSerde,
) (*processor.MeteredProcessor, *store.InMemoryWindowStoreWithChangelog, error) {
	hopWindow := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second).AdvanceBy(time.Duration(2) * time.Second)
	countStoreName := "auctionBidsCountStore"
	changelogName := countStoreName + "-changelog"
	changelog, err := sharedlog_stream.NewShardedSharedLogStream(h.env, changelogName, sp.NumOutPartition,
		commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return nil, nil, fmt.Errorf("NewShardedSharedLogStream failed: %v", err)
	}
	var vtSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: commtypes.Uint64Serde{},
		}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}
	countMp := &store.MaterializeParam{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: vtSerde,
		MsgSerde:   msgSerde,
		StoreName:  countStoreName,
		Changelog:  changelog,
		Comparable: concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
			l := lhs.(uint64)
			r := rhs.(uint64)
			if l < r {
				return -1
			} else if l == r {
				return 0
			} else {
				return 1
			}
		}),
		ParNum: sp.ParNum,
	}
	countWindowStore, err := store.NewInMemoryWindowStoreWithChangelog(
		hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
		hopWindow.MaxSize(), countMp,
	)
	if err != nil {
		return nil, nil, err
	}
	countProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(countWindowStore,
		processor.InitializerFunc(func() interface{} { return uint64(0) }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow))
	return countProc, countWindowStore, nil
}

type q5AuctionBidsProcessArg struct {
	countProc       *processor.MeteredProcessor
	groupByAuction  *processor.MeteredProcessor
	src             *processor.MeteredSource
	sink            *processor.MeteredSink
	output_stream   *sharedlog_stream.ShardedSharedLogStream
	trackParFunc    sharedlog_stream.TrackKeySubStreamFunc
	parNum          uint8
	numOutPartition uint8
}

func (h *q5AuctionBids) process(ctx context.Context, argsTmp interface{}) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*q5AuctionBidsProcessArg)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
			return h.currentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return h.currentOffset, &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	for _, msg := range gotMsgs {
		if msg.Msg.Value == nil {
			continue
		}
		h.currentOffset[args.src.TopicName()] = msg.LogSeqNum
		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("fail to extract timestamp: %v", err),
			}
		}
		msg.Msg.Timestamp = ts
		countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, countMsg := range countMsgs {
			// fmt.Fprintf(os.Stderr, "count msg ts: %v, ", countMsg.Timestamp)
			changeKeyedMsg, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			// fmt.Fprintf(os.Stderr, "changeKeyedMsg ts: %v\n", changeKeyedMsg[0].Timestamp)
			// par := uint8(hashSe(changeKeyedMsg[0].Key.(*ntypes.StartEndTime)) % uint32(args.numOutPartition))
			k := changeKeyedMsg[0].Key.(*ntypes.StartEndTime)
			h.cHashMu.RLock()
			parTmp, ok := h.cHash.Get(k)
			h.cHashMu.RUnlock()
			if !ok {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: "fail to get output partition",
				}
			}
			par := parTmp.(uint8)
			// fmt.Fprintf(os.Stderr, "key is %s, output to substream %d\n", k.String(), par)
			err = args.trackParFunc(ctx, k, args.sink.KeySerde(), args.sink.TopicName(), par)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("add topic partition failed: %v\n", err),
				}
			}
			err = args.sink.Sink(ctx, changeKeyedMsg[0], par, false)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
		}
	}
	return h.currentOffset, nil
}

func (h *q5AuctionBids) processQ5AuctionBids(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src, sink, err := h.getSrcSink(ctx, sp, msgSerde, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	countProc, countStore, err := h.getCountAggProc(sp, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	groupByAuction := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(msg commtypes.Message) (commtypes.Message, error) {
			key := msg.Key.(*commtypes.WindowedKey)
			value := msg.Value.(uint64)
			newKey := &ntypes.StartEndTime{
				StartTime: key.Window.Start(),
				EndTime:   key.Window.End(),
			}
			newVal := &ntypes.AuctionIdCount{
				AucId:     key.Key.(uint64),
				Count:     value,
				TimeStamp: msg.Timestamp,
			}
			return commtypes.Message{Key: newKey, Value: newVal, Timestamp: msg.Timestamp}, nil
		})))
	procArgs := &q5AuctionBidsProcessArg{
		countProc:       countProc,
		groupByAuction:  groupByAuction,
		src:             src,
		sink:            sink,
		output_stream:   output_stream,
		parNum:          sp.ParNum,
		numOutPartition: sp.NumOutPartition,
		trackParFunc:    sharedlog_stream.DefaultTrackSubstreamFunc,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	sharedlog_stream.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartition)

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         srcs,
			OutputStream: output_stream,
			QueryInput:   sp,
			TransactionalId: fmt.Sprintf("q5AuctionBids-%s-%d-%s", sp.InputTopicNames[0],
				sp.ParNum, sp.OutputTopicName),
			FixedOutParNum: 0,
			WindowStoreChangelogs: []*sharedlog_stream.WindowStoreChangelog{
				sharedlog_stream.NewWindowStoreChangelog(
					countStore,
					countStore.MaterializeParam().Changelog,
					countStore.KeyWindowTsSerde(),
					countStore.MaterializeParam().KeySerde,
					countStore.MaterializeParam().ValueSerde, 0),
			},
			KVChangelogs: nil,
			MsgSerde:     msgSerde,
			CHash:        h.cHash,
			CHashMu:      &h.cHashMu,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc sharedlog_stream.TrackKeySubStreamFunc) {
				procArgs.(*q5AuctionBidsProcessArg).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["count"] = countProc.GetLatency()
			ret.Latencies["changeKey"] = groupByAuction.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["count"] = countProc.GetLatency()
		ret.Latencies["changeKey"] = groupByAuction.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
