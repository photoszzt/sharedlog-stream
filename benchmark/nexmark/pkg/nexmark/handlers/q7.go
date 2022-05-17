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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/stream/processor/store_with_changelog"
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
	ctx context.Context,
	input *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
	msgSerde commtypes.MsgSerde,
) (*processor.MeteredSource, *sharedlog_stream.ConcurrentMeteredSink, error) {
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
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:      msgSerde,
		KeySerde:      commtypes.Uint64Serde{},
		ValueSerde:    bmSerde,
		FlushDuration: time.Duration(input.FlushMs) * time.Millisecond,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(input.WarmupS)*time.Second)
	sink := sharedlog_stream.NewConcurrentMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig),
		time.Duration(input.WarmupS)*time.Second)

	return src, sink, nil
}

type processQ7ProcessArgs struct {
	src                *processor.MeteredSource
	sink               *sharedlog_stream.ConcurrentMeteredSink
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
func (a *processQ7ProcessArgs) PushToAllSinks(ctx context.Context, msg commtypes.Message, parNum uint8, isControl bool) error {
	return a.sink.Sink(ctx, msg, parNum, isControl)
}
func (a *processQ7ProcessArgs) ParNum() uint8    { return a.parNum }
func (a *processQ7ProcessArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *processQ7ProcessArgs) FuncName() string { return a.funcName }
func (a *processQ7ProcessArgs) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}
func (a *processQ7ProcessArgs) ErrChan() chan error {
	return nil
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

func (h *query7Handler) procMsg(ctx context.Context, msg commtypes.Message, args *processQ7ProcessArgs) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	_, err = args.maxPriceBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	transformedMsgs, err := args.transformWithStore.ProcessAndReturn(ctx, msg)
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

	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				err := h.procMsgWithoutSink(ctx, subMsg, args)
				if err != nil {
					return err
				}
			}
		} else {
			if msg.Msg.Value == nil {
				continue
			}
			err := h.procMsgWithoutSink(ctx, msg.Msg, args)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (h *query7Handler) procMsgWithoutSink(ctx context.Context, msg commtypes.Message, args *processQ7RestoreArgs) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	_, err = args.maxPriceBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	transformedMsgs, err := args.transformWithStore.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	for _, tmsg := range transformedMsgs {
		_, err := args.filterTime.ProcessAndReturn(ctx, tmsg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *query7Handler) processQ7(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, input, true)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Assert(len(outputStreams) == 1, "expected only one output stream")
	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
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

	src, sink, err := h.getSrcSink(ctx, input, inputStream, outputStreams[0], msgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}

	tw, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxPriceBidStoreName := "max-price-bid-tab"
	var wstore store.WindowStore
	if input.TableType == uint8(store.IN_MEM) {
		mp := &store_with_changelog.MaterializeParam{
			KeySerde:   commtypes.Uint64Serde{},
			ValueSerde: vtSerde,
			StoreName:  maxPriceBidStoreName,
			ParNum:     input.ParNum,
			Changelog:  outputStreams[0],
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
		wstore, err = store_with_changelog.NewInMemoryWindowStoreWithChangelog(tw.MaxSize()+tw.GracePeriodMs(),
			tw.MaxSize(), false, mp)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
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
			return &common.FnOutput{Success: false, Message: err.Error()}
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
			agg, ok := aggregate.(*ntypes.PriceTime)
			if !ok {
				aggTmp := aggregate.(ntypes.PriceTime)
				agg = &aggTmp
			}
			if val.Bid.Price > agg.Price {
				return &ntypes.PriceTime{
					Price:    val.Bid.Price,
					DateTime: val.Bid.DateTime,
				}
			} else {
				return agg
			}

		}), tw), time.Duration(input.WarmupS)*time.Second)
	transformWithStore := processor.NewMeteredProcessor(NewQ7TransformProcessor(wstore), time.Duration(input.WarmupS)*time.Second)
	filterTime := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor(processor.PredicateFunc(func(m *commtypes.Message) (bool, error) {
			bm := m.Value.(*ntypes.BidAndMax)
			lb := bm.MaxDateTime - 10*1000
			return bm.DateTime >= lb && bm.DateTime <= bm.MaxDateTime, nil
		})), time.Duration(input.WarmupS)*time.Second)

	procArgs := &processQ7ProcessArgs{
		src:                src,
		sink:               sink,
		output_stream:      outputStreams[0],
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
		ProcessFunc:               h.process,
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
	}

	srcs := map[string]processor.Source{input.InputTopicNames[0]: src}
	if input.EnableTransaction {
		var wsc []*transaction.WindowStoreChangelog
		if input.TableType == uint8(store.IN_MEM) {
			wstore_mem := wstore.(*store_with_changelog.InMemoryWindowStoreWithChangelog)
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
					}, fmt.Sprintf("%s-%s-%d", h.funcName, wstore.Name(), input.ParNum), input.ParNum),
			}
		} else {
			panic("unrecognized table type")
		}
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:      procArgs,
			Env:           h.env,
			Srcs:          srcs,
			OutputStreams: outputStreams,
			QueryInput:    input,
			TransactionalId: fmt.Sprintf("%s-%s-%d-%s", h.funcName,
				input.InputTopicNames[0], input.ParNum, input.OutputTopicNames[0]),
			FixedOutParNum:        input.ParNum,
			MsgSerde:              msgSerde,
			WindowStoreChangelogs: wsc,
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
		ProcArgs:       procArgs,
		Duration:       time.Duration(input.Duration) * time.Second,
		Srcs:           srcs,
		ParNum:         input.ParNum,
		SerdeFormat:    commtypes.SerdeFormat(input.SerdeFormat),
		Env:            h.env,
		NumInPartition: input.NumInPartition,
		WarmupTime:     time.Duration(input.WarmupS) * time.Second,
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
