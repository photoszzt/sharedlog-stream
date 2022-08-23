package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
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

type q7MaxBid struct {
	env      types.Environment
	funcName string
}

const (
	Q7SizePerStore = 5 * 1024 * 1024
)

func NewQ7MaxBid(env types.Environment, funcName string) types.FuncHandler {
	return &q7MaxBid{
		env:      env,
		funcName: funcName,
	}
}

func (h *q7MaxBid) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.q7MaxBidByPrice(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q7MaxBid) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	seSerde, err := ntypes.GetStartEndTimeSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	inMsgSerde, err := commtypes.GetMsgSerdeG(serdeFormat, seSerde, eventSerde)
	if err != nil {
		return nil, nil, err
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, seSerde)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &producer_consumer.StreamConsumerConfigG[ntypes.StartEndTime, *ntypes.Event]{
		MsgSerde:    inMsgSerde,
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: serdeFormat,
	}
	outConfig := &producer_consumer.StreamSinkConfig[uint64, ntypes.StartEndTime]{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumerG(input_stream, inConfig, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig),
		warmup)
	src.SetInitialSource(false)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q7MaxBid) q7MaxBidByPrice(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks_arr, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx := processor.NewExecutionContext(srcs, sinks_arr, h.funcName, sp.ScaleEpoch, sp.ParNum)
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)

	maxBidByWinStoreName := "maxBidByWinKVStore"
	seSerde, err := ntypes.GetStartEndTimeSerdeG(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	msgSerde, err := processor.MsgSerdeWithValueTsG[ntypes.StartEndTime, uint64](serdeFormat, seSerde, commtypes.Uint64SerdeG{})
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]]().
		MessageSerde(msgSerde).
		StoreName(maxBidByWinStoreName).
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
	sizeOfVTs := commtypes.ValueTimestampGSize[uint64]{
		ValSizeFunc: commtypes.SizeOfUint64,
	}
	cacheStore := store.NewCachingKeyValueStoreG[ntypes.StartEndTime, commtypes.ValueTimestampG[uint64]](
		ctx, kvstore, ntypes.SizeOfStartEndTime, sizeOfVTs.SizeOfValueTimestamp, Q7SizePerStore)

	ectx.Via(processor.NewMeteredProcessor(
		processor.NewStreamAggregateProcessorG[ntypes.StartEndTime, *ntypes.Event, uint64]("maxBid",
			cacheStore, processor.InitializerFuncG[uint64](func() uint64 {
				return uint64(0)
			}),
			processor.AggregatorFuncG[ntypes.StartEndTime, *ntypes.Event, uint64](
				func(key ntypes.StartEndTime, v *ntypes.Event, agg uint64) uint64 {
					if v.Bid.Price > agg {
						return v.Bid.Price
					}
					return agg
				})))).
		Via(processor.NewTableToStreamProcessor()).
		Via(processor.NewMeteredProcessor(processor.NewStreamMapProcessor("remapKV",
			processor.MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
				return value, key, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(sinks_arr[0], &ectx))
	task := stream_task.NewStreamTaskBuilder().Build()
	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): cacheStore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName,
		sp.InputTopicNames[0], sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
