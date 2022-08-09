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
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
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
	var seSerde commtypes.SerdeG[ntypes.StartEndTime]
	var aucIdCountSerde commtypes.SerdeG[ntypes.AuctionIdCount]
	var aucIdCountMaxSerde commtypes.SerdeG[ntypes.AuctionIdCntMax]
	if serdeFormat == commtypes.JSON {
		seSerde = ntypes.StartEndTimeJSONSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerdeG{}
		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxJSONSerdeG{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = ntypes.StartEndTimeMsgpSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerdeG{}

		aucIdCountMaxSerde = ntypes.AuctionIdCntMaxMsgpSerdeG{}
	} else {
		return nil, nil,
			fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}
	inMsgSerde, err := commtypes.GetMsgSerdeG(serdeFormat, seSerde, aucIdCountSerde)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG(serdeFormat, seSerde, aucIdCountMaxSerde)
	if err != nil {
		return nil, nil, err
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	inConfig := &producer_consumer.StreamConsumerConfigG[ntypes.StartEndTime, ntypes.AuctionIdCount]{
		Timeout:     time.Duration(20) * time.Second,
		MsgSerde:    inMsgSerde,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig[ntypes.StartEndTime, ntypes.AuctionIdCntMax]{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumerG(input_stream,
		inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig),
		warmup)
	src.SetInitialSource(false)
	sink.MarkFinalOutput()
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q5MaxBid) processQ5MaxBid(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	maxBidStoreName := "maxBidsKVStore"
	seSerde, err := ntypes.GetStartEndTimeSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := processor.MsgSerdeWithValueTsG(serdeFormat,
		seSerde, commtypes.Uint64Serde{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[ntypes.StartEndTime, *commtypes.ValueTimestamp]().
		MessageSerde(msgSerde).
		StoreName(maxBidStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	compare := func(a, b ntypes.StartEndTime) bool {
		return ntypes.CompareStartEndTime(a, b) < 0
	}
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp, compare)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	maxBid := processor.NewMeteredProcessor(
		processor.NewStreamAggregateProcessorG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64]("maxBid",
			kvstore, processor.InitializerFuncG[uint64](func() uint64 {
				return uint64(0)
			}),
			processor.AggregatorFuncG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64](
				func(key ntypes.StartEndTime, v ntypes.AuctionIdCount, agg uint64) uint64 {
					if v.Count > agg {
						return v.Count
					}
					return agg
				})))
	stJoin := processor.NewMeteredProcessor(
		processor.NewStreamTableJoinProcessorG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64, ntypes.AuctionIdCntMax](kvstore,
			processor.ValueJoinerWithKeyFuncG[ntypes.StartEndTime, ntypes.AuctionIdCount, uint64, ntypes.AuctionIdCntMax](
				func(readOnlyKey ntypes.StartEndTime, lv ntypes.AuctionIdCount, rv uint64) ntypes.AuctionIdCntMax {
					return ntypes.AuctionIdCntMax{
						AucId:  lv.AucId,
						Count:  lv.Count,
						MaxCnt: rv,
					}
				})))
	chooseMaxCnt := processor.NewMeteredProcessor(
		processor.NewStreamFilterProcessor("chooseMaxCnt",
			processor.PredicateFunc(func(key, value interface{}) (bool, error) {
				v := value.(ntypes.AuctionIdCntMax)
				return v.Count >= v.MaxCnt, nil
			})))
	ectx := processor.NewExecutionContext(srcs,
		sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum)
	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask,
			argsTmp processor.ExecutionContext,
		) (*common.FnOutput, *commtypes.MsgAndSeq) {
			args := argsTmp.(*processor.BaseExecutionContext)
			return stream_task.CommonProcess(ctx, task, args, func(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
				// fmt.Fprintf(os.Stderr, "got msg with key: %v, val: %v, ts: %v\n", msg.Msg.Key, msg.Msg.Value, msg.Msg.Timestamp)
				_, err := maxBid.ProcessAndReturn(ctx, msg)
				if err != nil {
					return fmt.Errorf("maxBid err: %v", err)
				}
				joinedOutput, err := stJoin.ProcessAndReturn(ctx, msg)
				if err != nil {
					return fmt.Errorf("joined err: %v", err)
				}
				filteredMx, err := chooseMaxCnt.ProcessAndReturn(ctx, joinedOutput[0])
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
			})
		}).MarkFinalStage().Build()
	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): kvstore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).FixedOutParNum(sp.ParNum).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
