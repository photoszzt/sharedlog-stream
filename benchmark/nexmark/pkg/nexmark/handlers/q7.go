package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query7Handler struct {
	env           types.Environment
	currentOffset map[string]uint64
}

func NewQuery7(env types.Environment) types.FuncHandler {
	return &query7Handler{
		env:           env,
		currentOffset: make(map[string]uint64),
	}
}

func (h *query7Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.processQ7(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *query7Handler) getSrcSink(
	input *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredSource, *processor.MeteredSink, error) {
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	var bmSerde commtypes.Serde
	if input.SerdeFormat == uint8(commtypes.JSON) {
		bmSerde = ntypes.BidAndMaxJSONSerde{}
	} else if input.SerdeFormat == uint8(commtypes.MSGP) {
		bmSerde = ntypes.BidAndMaxMsgpSerde{}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", input.SerdeFormat)
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:   msgSerde,
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: bmSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))

	return src, sink, nil
}

type processQ7ProcessArgs struct {
	src                *processor.MeteredSource
	sink               *processor.MeteredSink
	output_stream      *sharedlog_stream.ShardedSharedLogStream
	maxPriceBid        *processor.MeteredProcessor
	transformWithStore *processor.MeteredProcessor
	filterTime         *processor.MeteredProcessor
	trackParFunc       sharedlog_stream.TrackKeySubStreamFunc
	parNum             uint8
}

func (h *query7Handler) process(
	ctx context.Context,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*processQ7ProcessArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
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
		_, err = args.maxPriceBid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		transformedMsgs, err := args.transformWithStore.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return h.currentOffset, &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, tmsg := range transformedMsgs {
			filtered, err := args.filterTime.ProcessAndReturn(ctx, tmsg)
			if err != nil {
				return h.currentOffset, &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			for _, fmsg := range filtered {
				err = args.sink.Sink(ctx, fmsg, args.parNum, false)
				if err != nil {
					return h.currentOffset, &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
			}
		}
	}
	return h.currentOffset, nil
}

func (h *query7Handler) processQ7(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	inputStream, outputStream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, input, true)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	var vtSerde commtypes.Serde
	var ptSerde commtypes.Serde
	if input.SerdeFormat == uint8(commtypes.JSON) {
		ptSerde = ntypes.PriceTimeJSONSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: ptSerde,
		}

	} else if input.SerdeFormat == uint8(commtypes.MSGP) {
		ptSerde = ntypes.PriceTimeMsgpSerde{}
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: ptSerde,
		}

	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}

	src, sink, err := h.getSrcSink(input, inputStream, outputStream, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	tw := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	maxPriceBidStoreName := "max-price-bid-tab"
	mp := &store.MaterializeParam{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: vtSerde,
		StoreName:  maxPriceBidStoreName,
		ParNum:     input.ParNum,
		Changelog:  outputStream,
		MsgSerde:   msgSerde,
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
	}
	store, err := store.NewInMemoryWindowStoreWithChangelog(tw.MaxSize()+tw.GracePeriodMs(),
		tw.MaxSize(), false, mp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	maxPriceBid := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return &ntypes.PriceTime{
				Price:    0,
				DateTime: 0,
			}
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := value.(*ntypes.Event)
			agg := aggregate.(*ntypes.PriceTime)
			if val.Bid.Price > agg.Price {
				return &ntypes.PriceTime{
					Price:    val.Bid.Price,
					DateTime: val.Bid.DateTime,
				}
			} else {
				return agg
			}

		}), tw))
	transformWithStore := processor.NewMeteredProcessor(NewQ7TransformProcessor(store))
	filterTime := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor(processor.PredicateFunc(func(m *commtypes.Message) (bool, error) {
			bm := m.Value.(*ntypes.BidAndMax)
			lb := bm.MaxDateTime - 10*1000
			return bm.DateTime >= lb && bm.DateTime <= bm.MaxDateTime, nil
		})))

	procArgs := &processQ7ProcessArgs{
		src:                src,
		sink:               sink,
		output_stream:      outputStream,
		parNum:             input.ParNum,
		maxPriceBid:        maxPriceBid,
		transformWithStore: transformWithStore,
		filterTime:         filterTime,
		trackParFunc:       sharedlog_stream.DefaultTrackSubstreamFunc,
	}
	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}
	if input.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[input.InputTopicNames[0]] = src
		streamTaskArgs := sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         srcs,
			OutputStream: outputStream,
			QueryInput:   input,
			TransactionalId: fmt.Sprintf("processQ7ProcessArgs-%s-%d-%s",
				input.InputTopicNames[0], input.NumInPartition, input.OutputTopicName),
			FixedOutParNum: input.ParNum,
			MsgSerde:       msgSerde,
			WindowStoreChangelogs: []*sharedlog_stream.WindowStoreChangelog{
				sharedlog_stream.NewWindowStoreChangelog(store,
					store.MaterializeParam().Changelog,
					store.KeyWindowTsSerde(),
					store.MaterializeParam().KeySerde,
					store.MaterializeParam().ValueSerde,
					store.MaterializeParam().ParNum),
			},
			CHash:   nil,
			CHashMu: nil,
		}
		ret := sharedlog_stream.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc sharedlog_stream.TrackKeySubStreamFunc) {
				procArgs.(*processQ7ProcessArgs).trackParFunc = trackParFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["maxPriceBid"] = maxPriceBid.GetLatency()
			ret.Latencies["transformWithStore"] = transformWithStore.GetLatency()
			ret.Latencies["filterTime"] = filterTime.GetLatency()
			ret.Consumed["src"] = src.GetCount()
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(input.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["maxPriceBid"] = maxPriceBid.GetLatency()
		ret.Latencies["transformWithStore"] = transformWithStore.GetLatency()
		ret.Latencies["filterTime"] = filterTime.GetLatency()
		ret.Consumed["src"] = src.GetCount()
	}
	return ret
}
