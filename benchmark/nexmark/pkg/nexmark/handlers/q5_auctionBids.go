package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5AuctionBids struct {
	env      types.Environment
	funcName string
}

func NewQ5AuctionBids(env types.Environment, funcName string) *q5AuctionBids {
	return &q5AuctionBids{
		env:      env,
		funcName: funcName,
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

func (h *q5AuctionBids) getSrcSink(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde,
) (*source_sink.MeteredSource, *source_sink.ConcurrentMeteredSyncSink, error) {

	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	var seSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	src := source_sink.NewMeteredSource(
		source_sink.NewShardedSharedLogStreamSource(input_stream, &source_sink.StreamSourceConfig{
			Timeout: time.Duration(5) * time.Second,
			KVMsgSerdes: commtypes.KVMsgSerdes{
				KeySerde: commtypes.Uint64Serde{},
				ValSerde: eventSerde,
				MsgSerde: msgSerde,
			},
		}), warmup)
	sink := source_sink.NewConcurrentMeteredSyncSink(
		source_sink.NewShardedSharedLogStreamSyncSink(output_streams[0], &source_sink.StreamSinkConfig{
			KVMsgSerdes: commtypes.KVMsgSerdes{
				MsgSerde: msgSerde,
				KeySerde: seSerde,
				ValSerde: aucIdCountSerde,
			},
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	src.SetInitialSource(false)
	return src, sink, nil
}

func (h *q5AuctionBids) getCountAggProc(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde,
) (*processor.MeteredProcessor, store.WindowStore, error) {
	hopWindow, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return nil, nil, err
	}
	hopWindow, err = hopWindow.AdvanceBy(time.Duration(2) * time.Second)
	if err != nil {
		return nil, nil, err
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
	var countWindowStore store.WindowStore
	countStoreName := "auctionBidsCountStore"
	if sp.TableType == uint8(store.IN_MEM) {
		comparable := concurrent_skiplist.CompareFunc(concurrent_skiplist.Uint64KeyCompare)
		inKVMsgSerdes := commtypes.KVMsgSerdes{
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: vtSerde,
			MsgSerde: msgSerde,
		}
		countMp, err := store_with_changelog.NewMaterializeParamBuilder().
			KVMsgSerdes(inKVMsgSerdes).
			StoreName(countStoreName).
			ParNum(sp.ParNum).
			SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
			StreamParam(commtypes.CreateStreamParam{
				Env:          h.env,
				NumPartition: sp.NumInPartition,
			}).Build()
		if err != nil {
			return nil, nil, err
		}
		countWindowStore, err = store_with_changelog.NewInMemoryWindowStoreWithChangelog(
			hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
			hopWindow.MaxSize(), false, comparable, countMp,
		)
		if err != nil {
			return nil, nil, err
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		client, err := store.InitMongoDBClient(ctx, sp.MongoAddr)
		if err != nil {
			return nil, nil, err
		}
		mkvs, err := store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
			Client:         client,
			CollectionName: countStoreName,
			DBName:         countStoreName,
			KeySerde:       nil,
			ValueSerde:     nil,
		})
		if err != nil {
			return nil, nil, err
		}
		byteStore, err := store.NewMongoDBSegmentedBytesStore(ctx, countStoreName,
			hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
			&store.WindowKeySchema{}, mkvs)
		if err != nil {
			return nil, nil, err
		}
		countWindowStore = store.NewSegmentedWindowStore(byteStore, false, hopWindow.MaxSize(),
			commtypes.Uint64Serde{}, vtSerde)
	}
	countProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(countWindowStore,
		processor.InitializerFunc(func() interface{} { return uint64(0) }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow), time.Duration(sp.WarmupS)*time.Second)
	return countProc, countWindowStore, nil
}

type q5AuctionBidsProcessArg struct {
	countProc      *processor.MeteredProcessor
	groupByAuction *processor.MeteredProcessor
	groupBy        *processor.GroupBy
	proc_interface.BaseExecutionContext
}

type q5AuctionBidsRestoreArg struct {
	countProc      processor.Processor
	groupByAuction processor.Processor
	src            source_sink.Source
	parNum         uint8
}

func (h *q5AuctionBids) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q5AuctionBidsProcessArg)
	countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("countProc err %v", err)
	}
	for _, countMsg := range countMsgs {
		// fmt.Fprintf(os.Stderr, "count msg ts: %v, ", countMsg.Timestamp)
		changeKeyedMsg, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
		if err != nil {
			return fmt.Errorf("groupByAuction err %v", err)
		}
		err = args.groupBy.GroupByAndProduce(ctx, changeKeyedMsg[0], args.TrackParFunc())
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *q5AuctionBids) processWithoutSink(ctx context.Context, argsTmp interface{}) error {
	args := argsTmp.(*q5AuctionBidsRestoreArg)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
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

func (h *q5AuctionBids) procMsgWithoutSink(ctx context.Context, msg commtypes.Message, args *q5AuctionBidsRestoreArg) error {
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractEventTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	countMsgs, err := args.countProc.ProcessAndReturn(ctx, msg)
	if err != nil {
		return err
	}
	for _, countMsg := range countMsgs {
		// fmt.Fprintf(os.Stderr, "count msg ts: %v, ", countMsg.Timestamp)
		_, err := args.groupByAuction.ProcessAndReturn(ctx, countMsg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (h *q5AuctionBids) processQ5AuctionBids(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	msgSerde, err := commtypes.GetMsgSerde(commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src, sink, err := h.getSrcSink(ctx, sp, msgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	countProc, countStore, err := h.getCountAggProc(ctx, sp, msgSerde)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	groupByAuction := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(msg commtypes.Message) (commtypes.Message, error) {
			key := msg.Key.(*commtypes.WindowedKey)
			value := msg.Value.(uint64)
			newKey := &ntypes.StartEndTime{
				StartTimeMs: key.Window.Start(),
				EndTimeMs:   key.Window.End(),
			}
			newVal := &ntypes.AuctionIdCount{
				AucId:  key.Key.(uint64),
				Count:  value,
				BaseTs: ntypes.BaseTs{Timestamp: msg.Timestamp},
			}
			return commtypes.Message{Key: newKey, Value: newVal, Timestamp: msg.Timestamp}, nil
		})), time.Duration(sp.WarmupS)*time.Second)
	groupBy := processor.NewGroupBy(sink)
	srcs := []source_sink.Source{src}
	sinks := []source_sink.Sink{sink}
	procArgs := &q5AuctionBidsProcessArg{
		countProc:      countProc,
		groupByAuction: groupByAuction,
		groupBy:        groupBy,
		BaseExecutionContext: proc_interface.NewExecutionContext(srcs,
			sinks, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			groupByAuction.StartWarmup()
			countProc.StartWarmup()
		}).Build()

	var wsc []*store_restore.WindowStoreChangelog
	if countStore.TableType() == store.IN_MEM {
		cstore := countStore.(*store_with_changelog.InMemoryWindowStoreWithChangelog)
		wsc = []*store_restore.WindowStoreChangelog{
			store_restore.NewWindowStoreChangelog(
				cstore,
				cstore.MaterializeParam().ChangelogManager(),
				cstore.KeyWindowTsSerde(),
				cstore.MaterializeParam().KVMsgSerdes(), 0),
		}
	} else if countStore.TableType() == store.MONGODB {
		wsc = []*store_restore.WindowStoreChangelog{
			store_restore.NewWindowStoreChangelogForExternalStore(countStore, src.Stream(),
				h.processWithoutSink, &q5AuctionBidsRestoreArg{
					countProc:      countProc.InnerProcessor(),
					groupByAuction: groupByAuction.InnerProcessor(),
					src:            src.InnerSource(),
					parNum:         sp.ParNum,
				}, fmt.Sprintf("%s-%s-%d", h.funcName, countStore.Name(), sp.ParNum), sp.ParNum),
		}
	} else {
		panic("unrecognized table type")
	}
	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["count"] = countProc.GetLatency()
		ret.Latencies["changeKey"] = groupByAuction.GetLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		WindowStoreChangelogs(wsc).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, sp.EnableTransaction, update_stats)
}
