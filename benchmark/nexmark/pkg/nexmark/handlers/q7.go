package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"

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
) (*source_sink.MeteredSource, *source_sink.ConcurrentMeteredSyncSink, error) {
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
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: bmSerde,
		},
		FlushDuration: time.Duration(input.FlushMs) * time.Millisecond,
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(input.WarmupS)*time.Second)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig),
		time.Duration(input.WarmupS)*time.Second)

	return src, sink, nil
}

type processQ7ProcessArgs struct {
	src                *source_sink.MeteredSource
	output_stream      *sharedlog_stream.ShardedSharedLogStream
	maxPriceBid        *processor.MeteredProcessor
	transformWithStore *processor.MeteredProcessor
	filterTime         *processor.MeteredProcessor
	trackParFunc       tran_interface.TrackKeySubStreamFunc
	proc_interface.BaseProcArgsWithSink
}

func (a *processQ7ProcessArgs) Source() source_sink.Source { return a.src }

type processQ7RestoreArgs struct {
	src                source_sink.Source
	maxPriceBid        processor.Processor
	transformWithStore processor.Processor
	filterTime         processor.Processor
	parNum             uint8
}

func (h *query7Handler) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*processQ7ProcessArgs)
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractEventTime()
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
			err = args.Sinks()[0].Produce(ctx, fmsg, args.ParNum(), false)
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
	ts, err := event.ExtractEventTime()
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
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, input)
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
	src.SetInitialSource(false)
	sink.MarkFinalOutput()

	tw, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxPriceBidStoreName := "max-price-bid-tab"
	var wstore store.WindowStore
	if input.TableType == uint8(store.IN_MEM) {
		comparable := concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
			l := lhs.(uint64)
			r := rhs.(uint64)
			if l < r {
				return -1
			} else if l == r {
				return 0
			} else {
				return 1
			}
		})
		kvmsgSerdes := commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: vtSerde,
			MsgSerde: msgSerde,
		}
		mp, err := store_with_changelog.NewMaterializeParamBuilder().
			KVMsgSerdes(kvmsgSerdes).StoreName(maxPriceBidStoreName).
			ParNum(input.ParNum).SerdeFormat(commtypes.SerdeFormat(input.SerdeFormat)).
			StreamParam(commtypes.CreateStreamParam{
				Env:          h.env,
				NumPartition: input.NumInPartition,
			}).Build()
		wstore, err = store_with_changelog.NewInMemoryWindowStoreWithChangelog(tw.MaxSize()+tw.GracePeriodMs(),
			tw.MaxSize(), false, comparable, mp)
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
		src:           src,
		output_stream: outputStreams[0],

		maxPriceBid:        maxPriceBid,
		transformWithStore: transformWithStore,
		filterTime:         filterTime,
		trackParFunc:       tran_interface.DefaultTrackSubstreamFunc,
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink([]source_sink.Sink{sink}, h.funcName,
			input.ScaleEpoch, input.ParNum),
	}
	task := transaction.StreamTask{
		ProcessFunc: func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		},
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
	}

	srcs := []source_sink.Source{src}
	sinks_arr := []source_sink.Sink{sink}
	var wsc []*transaction.WindowStoreChangelog
	if input.TableType == uint8(store.IN_MEM) {
		wstore_mem := wstore.(*store_with_changelog.InMemoryWindowStoreWithChangelog)
		wsc = []*transaction.WindowStoreChangelog{
			transaction.NewWindowStoreChangelog(wstore,
				wstore_mem.MaterializeParam().ChangelogManager(),
				wstore_mem.KeyWindowTsSerde(),
				wstore_mem.MaterializeParam().KVMsgSerdes(),
				wstore_mem.MaterializeParam().ParNum()),
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

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["maxPriceBid"] = maxPriceBid.GetLatency()
		ret.Latencies["transformWithStore"] = transformWithStore.GetLatency()
		ret.Latencies["filterTime"] = filterTime.GetLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	if input.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
			input.InputTopicNames[0], input.ParNum, input.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks_arr).
			WithWindowStoreChangelogs(wsc).
			WithFixedOutParNum(input.ParNum)
		benchutil.UpdateStreamTaskArgsTransaction(input, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, &task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr)
	benchutil.UpdateStreamTaskArgs(input, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
}
