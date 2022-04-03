package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/transaction"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type query7Handler struct {
	env      types.Environment
	funcName string
}

func NewQuery7(env types.Environment, funcName string) types.FuncHandler {
	return &query7Handler{
		env:      env,
		funcName: funcName,
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
	trackParFunc       transaction.TrackKeySubStreamFunc
	recordFinishFunc   transaction.RecordPrevInstanceFinishFunc
	funcName           string
	curEpoch           uint64
	parNum             uint8
}

func (a *processQ7ProcessArgs) Source() processor.Source { return a.src }
func (a *processQ7ProcessArgs) Sink() processor.Sink     { return a.sink }
func (a *processQ7ProcessArgs) ParNum() uint8            { return a.parNum }
func (a *processQ7ProcessArgs) CurEpoch() uint64         { return a.curEpoch }
func (a *processQ7ProcessArgs) FuncName() string         { return a.funcName }
func (a *processQ7ProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

type processQ7RestoreArgs struct {
	src                processor.Source
	maxPriceBid        processor.Processor
	transformWithStore processor.Processor
	filterTime         processor.Processor
	parNum             uint8
}

func (h *query7Handler) process(
	ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*processQ7ProcessArgs)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			return fmt.Errorf("fail to extract timestamp: %v", err)
		}
		msg.Msg.Timestamp = ts
		_, err = args.maxPriceBid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return err
		}
		transformedMsgs, err := args.transformWithStore.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return err
		}
		for _, tmsg := range transformedMsgs {
			filtered, err := args.filterTime.ProcessAndReturn(ctx, tmsg)
			if err != nil {
				return err
			}
			for _, fmsg := range filtered {
				err = args.sink.Sink(ctx, fmsg, args.parNum, false)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (h *query7Handler) processWithoutSink(
	ctx context.Context,
	argsTmp interface{},
) error {
	args := argsTmp.(*processQ7RestoreArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return nil
		}
		return err
	}

	for _, msg := range gotMsgs {
		if msg.Msg.Value == nil {
			continue
		}
		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			return fmt.Errorf("fail to extract timestamp: %v", err)
		}
		msg.Msg.Timestamp = ts
		_, err = args.maxPriceBid.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return err
		}
		transformedMsgs, err := args.transformWithStore.ProcessAndReturn(ctx, msg.Msg)
		if err != nil {
			return err
		}
		for _, tmsg := range transformedMsgs {
			_, err := args.filterTime.ProcessAndReturn(ctx, tmsg)
			if err != nil {
				return err
			}
		}
	}
	return nil
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

	tw, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	maxPriceBidStoreName := "max-price-bid-tab"
	var wstore store.WindowStore
	if input.TableType == uint8(store.IN_MEM) {
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
		wstore, err = store.NewInMemoryWindowStoreWithChangelog(tw.MaxSize()+tw.GracePeriodMs(),
			tw.MaxSize(), false, mp)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
	} else if input.TableType == uint8(store.MONGODB) {
		client, err := store.InitMongoDBClient(ctx, input.MongoAddr)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		wstore, err = processor.CreateMongoDBWindoeTable(ctx, maxPriceBidStoreName,
			client, tw.MaxSize()+tw.GracePeriodMs(), tw.MaxSize(),
			commtypes.Uint64Serde{}, vtSerde)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
	} else {
		panic("unrecognized table type")
	}

	maxPriceBid := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(wstore,
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
	transformWithStore := processor.NewMeteredProcessor(NewQ7TransformProcessor(wstore))
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
		trackParFunc:       transaction.DefaultTrackSubstreamFunc,
		recordFinishFunc:   transaction.DefaultRecordPrevInstanceFinishFunc,
		curEpoch:           input.ScaleEpoch,
		funcName:           h.funcName,
	}
	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}
	if input.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[input.InputTopicNames[0]] = src
		var wsc []*transaction.WindowStoreChangelog
		if input.TableType == uint8(store.IN_MEM) {
			wstore_mem := wstore.(*store.InMemoryWindowStoreWithChangelog)
			wsc = []*transaction.WindowStoreChangelog{
				transaction.NewWindowStoreChangelog(wstore,
					wstore_mem.MaterializeParam().Changelog,
					wstore_mem.KeyWindowTsSerde(),
					wstore_mem.MaterializeParam().KeySerde,
					wstore_mem.MaterializeParam().ValueSerde,
					wstore_mem.MaterializeParam().ParNum),
			}
		} else if input.TableType == uint8(store.MONGODB) {
			wsc = []*transaction.WindowStoreChangelog{
				transaction.NewWindowStoreChangelogForExternalStore(wstore, inputStream, h.processWithoutSink,
					&processQ7RestoreArgs{
						src:                src.InnerSource(),
						parNum:             input.ParNum,
						maxPriceBid:        maxPriceBid.InnerProcessor(),
						transformWithStore: transformWithStore.InnerProcessor(),
						filterTime:         filterTime.InnerProcessor(),
					}, input.ParNum),
			}
		} else {
			panic("unrecognized table type")
		}
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:     procArgs,
			Env:          h.env,
			Srcs:         srcs,
			OutputStream: outputStream,
			QueryInput:   input,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName,
				input.InputTopicNames[0], input.NumInPartition, input.OutputTopicName),
			FixedOutParNum:        input.ParNum,
			MsgSerde:              msgSerde,
			WindowStoreChangelogs: wsc,
			CHash:                 nil,
			CHashMu:               nil,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*processQ7ProcessArgs).trackParFunc = trackParFunc
				procArgs.(*processQ7ProcessArgs).recordFinishFunc = recordFinishFunc
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
	streamTaskArgs := transaction.StreamTaskArgs{
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
