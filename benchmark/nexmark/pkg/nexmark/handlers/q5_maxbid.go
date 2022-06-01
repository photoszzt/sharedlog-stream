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
	"sharedlog-stream/pkg/treemap"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type q5MaxBid struct {
	env      types.Environment
	funcName string
}

func NewQ5MaxBid(env types.Environment, funcName string) types.FuncHandler {
	return &q5MaxBid{
		env:      env,
		funcName: funcName,
	}
}

func (h *q5MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.processQ5MaxBid(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5MaxBid) getSrcSink(ctx context.Context,
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*source_sink.MeteredSource, *source_sink.ConcurrentMeteredSyncSink, commtypes.KVMsgSerdes, error) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	var seSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	var aucIdCountMaxSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}

		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}

		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxMsgpSerde{}
	} else {
		return nil, nil, commtypes.KVMsgSerdes{},
			fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, commtypes.KVMsgSerdes{}, err
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: seSerde,
		ValSerde: aucIdCountSerde,
		MsgSerde: msgSerde,
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout:     time.Duration(20) * time.Second,
		KVMsgSerdes: kvmsgSerdes,
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: seSerde,
			ValSerde: aucIdCountMaxSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(false)
	sink.MarkFinalOutput()
	return src, sink, kvmsgSerdes, nil
}

type q5MaxBidProcessArgs struct {
	maxBid       *processor.MeteredProcessor
	stJoin       *processor.MeteredProcessor
	chooseMaxCnt *processor.MeteredProcessor
	proc_interface.BaseProcArgsWithSrcSink
}

type q5MaxBidRestoreArgs struct {
	maxBid       processor.Processor
	stJoin       processor.Processor
	chooseMaxCnt processor.Processor
	src          source_sink.Source
	parNum       uint8
}

func (h *q5MaxBid) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q5MaxBidProcessArgs)
	aic := msg.Value.(*ntypes.AuctionIdCount)
	ts, err := aic.ExtractEventTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
	_, err = args.maxBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("maxBid err: %v", err)
	}
	joinedOutput, err := args.stJoin.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("joined err: %v", err)
	}
	filteredMx, err := args.chooseMaxCnt.ProcessAndReturn(ctx, joinedOutput[0])
	if err != nil {
		return fmt.Errorf("filteredMx err: %v", err)
	}
	for _, filtered := range filteredMx {
		err = args.Sinks()[0].Produce(ctx, filtered, args.ParNum(), false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *q5MaxBid) processWithoutSink(ctx context.Context, argsTmp interface{}) error {
	args := argsTmp.(*q5MaxBidRestoreArgs)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return nil
		}
		return fmt.Errorf("consume err: %v", err)
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

func (h *q5MaxBid) procMsgWithoutSink(ctx context.Context, msg commtypes.Message, args *q5MaxBidRestoreArgs) error {
	aic := msg.Value.(*ntypes.AuctionIdCount)
	ts, err := aic.ExtractEventTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
	_, err = args.maxBid.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("maxBid err: %v", err)
	}
	joinedOutput, err := args.stJoin.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("joined err: %v", err)
	}
	_, err = args.chooseMaxCnt.ProcessAndReturn(ctx, joinedOutput[0])
	if err != nil {
		return fmt.Errorf("filteredMx err: %v", err)
	}
	return nil
}

func (h *q5MaxBid) processQ5MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	vtSerde, err := commtypes.GetValueTsSerde(serdeFormat, commtypes.Uint64Serde{}, commtypes.Uint64Serde{})
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	src, sink, srcKVMsgSerdes, err := h.getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBidStoreName := "maxBidsKVStore"
	var kvstore store.KeyValueStore
	warmup := time.Duration(sp.WarmupS) * time.Second
	if sp.TableType == uint8(store.IN_MEM) {
		inKVMsgSerdes := commtypes.KVMsgSerdes{
			KeySerde: srcKVMsgSerdes.KeySerde,
			ValSerde: vtSerde,
			MsgSerde: srcKVMsgSerdes.MsgSerde,
		}
		mp, err := store_with_changelog.NewMaterializeParamBuilder().
			KVMsgSerdes(inKVMsgSerdes).
			StoreName(maxBidStoreName).
			ParNum(sp.ParNum).
			SerdeFormat(serdeFormat).
			StreamParam(commtypes.CreateStreamParam{
				Env:          h.env,
				NumPartition: sp.NumInPartition,
			}).Build()
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		compare := func(a, b treemap.Key) int {
			ka := a.(*ntypes.StartEndTime)
			kb := b.(*ntypes.StartEndTime)
			return ntypes.CompareStartEndTime(ka, kb)
		}
		kvstore = store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare, warmup)
	} else if sp.TableType == uint8(store.MONGODB) {
		client, err := store.InitMongoDBClient(ctx, sp.MongoAddr)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		kvstore, err = store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
			Client:         client,
			CollectionName: maxBidStoreName,
			DBName:         maxBidStoreName,
			KeySerde:       srcKVMsgSerdes.KeySerde,
			ValueSerde:     vtSerde,
		})
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	} else {
		panic("unrecognized table type")
	}
	maxBid := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(kvstore, processor.InitializerFunc(func() interface{} {
		return uint64(0)
	}), processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
		v := value.(*ntypes.AuctionIdCount)
		agg := aggregate.(uint64)
		if v.Count > agg {
			return v.Count
		}
		return agg
	})), warmup)
	stJoin := processor.NewMeteredProcessor(processor.NewStreamTableJoinProcessor(maxBidStoreName, kvstore,
		processor.ValueJoinerWithKeyFunc(
			func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
				lv := leftValue.(*ntypes.AuctionIdCount)
				rv := rightValue.(commtypes.ValueTimestamp)
				return &ntypes.AuctionIdCntMax{
					AucId:  lv.AucId,
					Count:  lv.Count,
					MaxCnt: rv.Value.(uint64),
				}
			})), warmup)
	chooseMaxCnt := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor(
			processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
				v := msg.Value.(*ntypes.AuctionIdCntMax)
				return v.Count >= v.MaxCnt, nil
			})), warmup)

	sinks_arr := []source_sink.Sink{sink}
	procArgs := &q5MaxBidProcessArgs{
		maxBid:       maxBid,
		stJoin:       stJoin,
		chooseMaxCnt: chooseMaxCnt,
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(src,
			sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := transaction.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *transaction.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ProcArgsWithSrcSink)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
			maxBid.StartWarmup()
			stJoin.StartWarmup()
			chooseMaxCnt.StartWarmup()
		}).Build()

	srcs := []source_sink.Source{src}
	var kvc []*transaction.KVStoreChangelog
	if sp.TableType == uint8(store.IN_MEM) {
		kvstore_mem := kvstore.(*store_with_changelog.KeyValueStoreWithChangelog)
		mp := kvstore_mem.MaterializeParam()
		kvc = []*transaction.KVStoreChangelog{
			transaction.NewKVStoreChangelog(kvstore, mp.ChangelogManager(),
				mp.KVMsgSerdes(), sp.ParNum),
		}
	} else if sp.TableType == uint8(store.MONGODB) {
		// TODO: MONGODB
		kvc = []*transaction.KVStoreChangelog{
			transaction.NewKVStoreChangelogForExternalStore(kvstore, input_stream, h.processWithoutSink, &q5MaxBidRestoreArgs{
				maxBid:       maxBid.InnerProcessor(),
				stJoin:       stJoin.InnerProcessor(),
				chooseMaxCnt: chooseMaxCnt.InnerProcessor(),
				src:          src.InnerSource(),
				parNum:       sp.ParNum,
			}, fmt.Sprintf("%s-%s-%d", h.funcName, kvstore.Name(), sp.ParNum), sp.ParNum),
		}
	} else {
		panic("unrecognized table type")
	}

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["maxBid"] = maxBid.GetLatency()
		ret.Latencies["stJoin"] = stJoin.GetLatency()
		ret.Latencies["chooseMaxCnt"] = chooseMaxCnt.GetLatency()
		ret.Latencies["eventTimeLatency"] = sink.GetEventTimeLatency()
		ret.Counts["src"] = src.GetCount()
		ret.Counts["sink"] = sink.GetCount()
	}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
			sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks_arr).
			WithKVChangelogs(kvc)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks_arr).
		WithKVChangelogs(kvc)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
}
