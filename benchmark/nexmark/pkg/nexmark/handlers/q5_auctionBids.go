package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/control_channel"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5AuctionBids struct {
	env      types.Environment
	cHashMu  sync.RWMutex
	cHash    *hash.ConsistentHash
	funcName string
}

func NewQ5AuctionBids(env types.Environment, funcName string) *q5AuctionBids {
	return &q5AuctionBids{
		env:      env,
		cHash:    hash.NewConsistentHash(),
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

func (h *q5AuctionBids) getSrcSink(ctx context.Context, sp *common.QueryInput,
	msgSerde commtypes.MsgSerde, input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.ConcurrentMeteredSyncSink, error) {
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
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout:     time.Duration(5) * time.Second,
		KVMsgSerdes: kvmsgSerdes,
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: seSerde,
			ValSerde: aucIdCountSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
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
	output_stream  *sharedlog_stream.ShardedSharedLogStream
	proc_interface.BaseProcArgsWithSrcSink
	numOutPartition uint8
}

type q5AuctionBidsRestoreArg struct {
	countProc      processor.Processor
	groupByAuction processor.Processor
	src            source_sink.Source
	parNum         uint8
}

func (h *q5AuctionBids) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q5AuctionBidsProcessArg)
	event := msg.Value.(*ntypes.Event)
	ts, err := event.ExtractEventTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
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
		// fmt.Fprintf(os.Stderr, "changeKeyedMsg ts: %v\n", changeKeyedMsg[0].Timestamp)
		// par := uint8(hashSe(changeKeyedMsg[0].Key.(*ntypes.StartEndTime)) % uint32(args.numOutPartition))
		k := changeKeyedMsg[0].Key.(*ntypes.StartEndTime)
		h.cHashMu.RLock()
		parTmp, ok := h.cHash.Get(k)
		h.cHashMu.RUnlock()
		if !ok {
			return xerrors.New("fail to get output partition")
		}
		par := parTmp.(uint8)
		// fmt.Fprintf(os.Stderr, "key is %s, output to substream %d\n", k.String(), par)
		err = args.TrackParFunc()(ctx, k, args.Sinks()[0].KeySerde(), args.Sinks()[0].TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = args.Sinks()[0].Produce(ctx, changeKeyedMsg[0], par, false)
		if err != nil {
			return fmt.Errorf("sink err %v", err)
		}
	}
	return nil
}

func (h *q5AuctionBids) processWithoutSink(ctx context.Context, argsTmp interface{}) error {
	args := argsTmp.(*q5AuctionBidsRestoreArg)
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
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	src, sink, err := h.getSrcSink(ctx, sp, msgSerde, input_stream, output_streams[0])
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
				StartTime: key.Window.Start(),
				EndTime:   key.Window.End(),
			}
			newVal := &ntypes.AuctionIdCount{
				AucId:  key.Key.(uint64),
				Count:  value,
				BaseTs: ntypes.BaseTs{Timestamp: msg.Timestamp},
			}
			return commtypes.Message{Key: newKey, Value: newVal, Timestamp: msg.Timestamp}, nil
		})), time.Duration(sp.WarmupS)*time.Second)
	procArgs := &q5AuctionBidsProcessArg{
		countProc:       countProc,
		groupByAuction:  groupByAuction,
		output_stream:   output_streams[0],
		numOutPartition: sp.NumOutPartitions[0],
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(src,
			[]source_sink.Sink{sink}, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}

	task := transaction.StreamTask{
		ProcessFunc: func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		},
		CurrentOffset:             make(map[string]uint64),
		CommitEveryForAtLeastOnce: common.CommitDuration,
		PauseFunc:                 nil,
		ResumeFunc:                nil,
		InitFunc: func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			groupByAuction.StartWarmup()
			countProc.StartWarmup()
		},
	}

	control_channel.SetupConsistentHash(&h.cHashMu, h.cHash, sp.NumOutPartitions[0])
	srcs := []source_sink.Source{src}
	sinks := []source_sink.Sink{sink}
	var wsc []*transaction.WindowStoreChangelog
	if countStore.TableType() == store.IN_MEM {
		cstore := countStore.(*store_with_changelog.InMemoryWindowStoreWithChangelog)
		wsc = []*transaction.WindowStoreChangelog{
			transaction.NewWindowStoreChangelog(
				cstore,
				cstore.MaterializeParam().ChangelogManager(),
				cstore.KeyWindowTsSerde(),
				cstore.MaterializeParam().KVMsgSerdes(), 0),
		}
	} else if countStore.TableType() == store.MONGODB {
		wsc = []*transaction.WindowStoreChangelog{
			transaction.NewWindowStoreChangelogForExternalStore(countStore, input_stream,
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
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
			sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks).
			WithWindowStoreChangelogs(wsc)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, &task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks).
		WithWindowStoreChangelogs(wsc)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
}
