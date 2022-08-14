package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/ntypes"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/exp/slices"
)

type q6Avg struct {
	env      types.Environment
	funcName string
}

func NewQ6Avg(env types.Environment, funcName string) *q6Avg {
	return &q6Avg{
		env:      env,
		funcName: funcName,
	}
}

func (h *q6Avg) getExecutionCtx(ctx context.Context, sp *common.QueryInput) (processor.BaseExecutionContext, error) {
	inputStream, outputStreams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	ptSerde, err := ntypes.GetPriceTimeSerde(serdeFormat)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	changeSerde, err := commtypes.GetChangeSerdeG(serdeFormat, ptSerde)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	inMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, changeSerde)
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	outMsgSerde, err := commtypes.GetMsgSerdeG[uint64, float64](serdeFormat, commtypes.Uint64SerdeG{}, commtypes.Float64SerdeG{})
	if err != nil {
		return processor.BaseExecutionContext{}, fmt.Errorf("get msg serde err: %v", err)
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumerG(inputStream,
		&producer_consumer.StreamConsumerConfigG[uint64, commtypes.Change]{
			Timeout:     common.SrcConsumeTimeout,
			MsgSerde:    inMsgSerde,
			SerdeFormat: serdeFormat,
		}, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return processor.BaseExecutionContext{}, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink := producer_consumer.NewMeteredProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(outputStreams[0], &producer_consumer.StreamSinkConfig[uint64, float64]{
			MsgSerde:      outMsgSerde,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	sink.MarkFinalOutput()
	ectx := processor.NewExecutionContext([]producer_consumer.MeteredConsumerIntr{src},
		[]producer_consumer.MeteredProducerIntr{sink}, h.funcName, sp.ScaleEpoch, sp.ParNum)
	return ectx, nil
}

func (h *q6Avg) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.Q6Avg(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return common.CompressData(encodedOutput), nil
}

func (h *q6Avg) Q6Avg(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	ectx, err := h.getExecutionCtx(ctx, sp)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	aggStoreName := "q6AggKVStore"
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	ptlSerde, err := ntypes.GetPriceTimeListSerde(serdeFormat)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	storeMsgSerde, err := processor.MsgSerdeWithValueTsG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, ptlSerde)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *commtypes.ValueTimestamp]().
		MessageSerde(storeMsgSerde).
		StoreName(aggStoreName).
		ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
			TimeOut:       common.SrcConsumeTimeout,
		}).Build()
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	kvstore, err := store_with_changelog.CreateInMemorySkipmapKVTableWithChangelogG(mp, store.Uint64LessFunc)
	if err != nil {
		return common.GenErrFnOutput(err)
	}
	maxSize := 10
	ectx.
		Via(processor.NewMeteredProcessor(
			processor.NewTableAggregateProcessorG[uint64, ntypes.PriceTime, ntypes.PriceTimeList]("q6Agg", kvstore,
				processor.InitializerFuncG[ntypes.PriceTimeList](func() ntypes.PriceTimeList {
					return ntypes.PriceTimeList{
						PTList: make([]ntypes.PriceTime, 0),
					}
				}),
				processor.AggregatorFuncG[uint64, ntypes.PriceTime, ntypes.PriceTimeList](
					func(key uint64, value ntypes.PriceTime, aggregate ntypes.PriceTimeList) ntypes.PriceTimeList {
						aggregate.PTList = append(aggregate.PTList, value)
						slices.SortFunc(aggregate.PTList, store.LessFunc[ntypes.PriceTime](func(a, b ntypes.PriceTime) bool {
							return ntypes.ComparePriceTime(a, b) < 0
						}))
						// debug.Fprintf(os.Stderr, "[ADD] agg before delete: %v\n", agg.PTList)
						if len(aggregate.PTList) > maxSize {
							aggregate.PTList = ntypes.Delete(aggregate.PTList, 0, 1)
						}
						// debug.Fprintf(os.Stderr, "[ADD] agg after delete: %v\n", agg.PTList)
						return aggregate
					}),
				processor.AggregatorFuncG[uint64, ntypes.PriceTime, ntypes.PriceTimeList](
					func(key uint64, value ntypes.PriceTime, agg ntypes.PriceTimeList) ntypes.PriceTimeList {
						if len(agg.PTList) > 0 {
							// debug.Fprintf(os.Stderr, "[RM] element to delete: %+v\n", value)
							val := ntypes.CastToPriceTimePtr(value)
							agg.PTList = ntypes.RemoveMatching(agg.PTList, val)
							// debug.Fprintf(os.Stderr, "[RM] after delete agg: %v\n", agg)
						}
						return agg
					}),
			))).
		Via(processor.NewMeteredProcessor(processor.NewTableMapValuesProcessor("avg",
			processor.ValueMapperWithKeyFunc(func(key, value interface{}) (interface{}, error) {
				pt := value.(ntypes.PriceTimeList)
				sum := uint64(0)
				l := len(pt.PTList)
				for _, p := range pt.PTList {
					sum += p.Price
				}
				return float64(sum) / float64(l), nil
			})))).
		Via(processor.NewTableToStreamProcessor()).
		Via(processor.NewFixedSubstreamOutputProcessor(ectx.Producers()[0], sp.ParNum))
	task := stream_task.NewStreamTaskBuilder().MarkFinalStage().Build()

	kvc := map[string]store.KeyValueStoreOpWithChangelog{kvstore.ChangelogTopicName(): kvstore}
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)).
		KVStoreChangelogs(kvc).
		FixedOutParNum(sp.ParNum).
		Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
