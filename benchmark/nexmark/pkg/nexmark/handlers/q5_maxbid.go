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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/treemap"
	"time"

	"cs.utexas.edu/zjia/faas/types"
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

func (h *q5MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

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
		return nil, nil,
			fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout: time.Duration(20) * time.Second,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: seSerde,
			ValSerde: aucIdCountSerde,
			MsgSerde: msgSerde,
		},
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: seSerde,
			ValSerde: aucIdCountMaxSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig),
		warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig),
		warmup)
	src.SetInitialSource(false)
	sink.MarkFinalOutput()
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

type q5MaxBidProcessArgs struct {
	maxBid       *processor.MeteredProcessor
	stJoin       *processor.MeteredProcessor
	chooseMaxCnt *processor.MeteredProcessor
	processor.BaseExecutionContext
}

func (h *q5MaxBid) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*q5MaxBidProcessArgs)
	// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
	_, err := args.maxBid.ProcessAndReturn(ctx, msg)
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
		err = args.Producers()[0].Produce(ctx, filtered, args.SubstreamNum(), false)
		if err != nil {
			return fmt.Errorf("sink err: %v", err)
		}
	}
	return nil
}

func (h *q5MaxBid) processQ5MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	vtSerde, err := commtypes.GetValueTsSerde(serdeFormat, commtypes.Uint64Serde{}, commtypes.Uint64Serde{})
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	srcKVMsgSerdes := srcs[0].KVMsgSerdes()
	maxBidStoreName := "maxBidsKVStore"
	var kvstore store.KeyValueStore
	warmup := time.Duration(sp.WarmupS) * time.Second
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
		}).Build(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(*ntypes.StartEndTime)
		kb := b.(*ntypes.StartEndTime)
		return ntypes.CompareStartEndTime(ka, kb)
	}
	kvstore = store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare, warmup)
	maxBid := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor("maxBid",
		kvstore, processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}), processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			v := value.(*ntypes.AuctionIdCount)
			agg := aggregate.(uint64)
			if v.Count > agg {
				return v.Count
			}
			return agg
		})))
	stJoin := processor.NewMeteredProcessor(processor.NewStreamTableJoinProcessor(kvstore,
		processor.ValueJoinerWithKeyFunc(
			func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
				lv := leftValue.(*ntypes.AuctionIdCount)
				rv := rightValue.(commtypes.ValueTimestamp)
				return &ntypes.AuctionIdCntMax{
					AucId:  lv.AucId,
					Count:  lv.Count,
					MaxCnt: rv.Value.(uint64),
				}
			})))
	chooseMaxCnt := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor("chooseMaxCnt",
			processor.PredicateFunc(func(msg *commtypes.Message) (bool, error) {
				v := msg.Value.(*ntypes.AuctionIdCntMax)
				return v.Count >= v.MaxCnt, nil
			})))
	procArgs := &q5MaxBidProcessArgs{
		maxBid:       maxBid,
		stJoin:       stJoin,
		chooseMaxCnt: chooseMaxCnt,
		BaseExecutionContext: processor.NewExecutionContext(srcs,
			sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum),
	}
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).Build()
	kvc := []*store_restore.KVStoreChangelog{
		store_restore.NewKVStoreChangelog(kvstore, mp.ChangelogManager(), sp.ParNum),
	}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).
		KVStoreChangelogs(kvc).FixedOutParNum(sp.ParNum).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
