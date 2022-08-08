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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
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

func (h *q5AuctionBids) getSrcSink(ctx context.Context, sp *common.QueryInput,
) ([]producer_consumer.MeteredConsumerIntr, []producer_consumer.MeteredProducerIntr, error) {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")

	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	var seSerde commtypes.SerdeG[ntypes.StartEndTime]
	var aucIdCountSerde commtypes.SerdeG[ntypes.AuctionIdCount]
	if serdeFormat == commtypes.JSON {
		seSerde = ntypes.StartEndTimeJSONSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerdeG{}
	} else if serdeFormat == commtypes.MSGP {
		seSerde = ntypes.StartEndTimeMsgpSerdeG{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerdeG{}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	eventSerde, err := ntypes.GetEventSerdeG(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	srcMsgSerde, err := commtypes.GetMsgSerdeG[uint64](serdeFormat, commtypes.Uint64SerdeG{}, eventSerde)
	if err != nil {
		return nil, nil, err
	}
	sinkMsgSerde, err := commtypes.GetMsgSerdeG(serdeFormat, seSerde, aucIdCountSerde)
	if err != nil {
		return nil, nil, err
	}
	consumer, err := producer_consumer.NewShardedSharedLogStreamConsumerG(input_stream, &producer_consumer.StreamConsumerConfigG[uint64, *ntypes.Event]{
		Timeout:     time.Duration(5) * time.Second,
		MsgSerde:    srcMsgSerde,
		SerdeFormat: serdeFormat,
	}, sp.NumSubstreamProducer[0], sp.ParNum)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(consumer, warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], &producer_consumer.StreamSinkConfig[ntypes.StartEndTime, ntypes.AuctionIdCount]{
			MsgSerde:      sinkMsgSerde,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	src.SetInitialSource(false)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q5AuctionBids) getCountAggProc(ctx context.Context, sp *common.QueryInput,
) (*processor.MeteredProcessor, []store.WindowStoreOpWithChangelog, error) {
	hopWindow, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return nil, nil, err
	}
	hopWindow, err = hopWindow.AdvanceBy(time.Duration(2) * time.Second)
	if err != nil {
		return nil, nil, err
	}
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := processor.MsgSerdeWithValueTsG[uint64](serdeFormat,
		commtypes.Uint64SerdeG{}, commtypes.Uint64Serde{})
	if err != nil {
		return nil, nil, err
	}
	countStoreName := "auctionBidsCountStore"
	comparable := concurrent_skiplist.CompareFunc(concurrent_skiplist.Uint64KeyCompare)
	countMp, err := store_with_changelog.NewMaterializeParamBuilder[uint64, *commtypes.ValueTimestamp]().
		MessageSerde(msgSerde).StoreName(countStoreName).ParNum(sp.ParNum).
		SerdeFormat(serdeFormat).
		ChangelogManagerParam(commtypes.CreateChangelogManagerParam{
			Env:           h.env,
			NumPartition:  sp.NumInPartition,
			TimeOut:       common.SrcConsumeTimeout,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}).Build()
	if err != nil {
		return nil, nil, err
	}
	countWindowStore, err := store_with_changelog.NewInMemoryWindowStoreWithChangelog(
		hopWindow, false, comparable, countMp)
	if err != nil {
		return nil, nil, err
	}
	countProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(
		"countProc", countWindowStore,
		processor.InitializerFunc(func() interface{} { return uint64(0) }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow))
	wsc := []store.WindowStoreOpWithChangelog{countWindowStore}
	return countProc, wsc, nil
}

func (h *q5AuctionBids) processQ5AuctionBids(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	srcs, sinks, err := h.getSrcSink(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx := processor.NewExecutionContext(srcs,
		sinks, h.funcName, sp.ScaleEpoch, sp.ParNum)
	countProc, wsc, err := h.getCountAggProc(ctx, sp)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	ectx.Via(countProc).
		Via(processor.NewTableToStreamProcessor()).
		Via(processor.NewMeteredProcessor(processor.NewStreamMapProcessor("groupByAuction",
			processor.MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
				k := key.(*commtypes.WindowedKey)
				v := value.(uint64)
				newKey := ntypes.StartEndTime{
					StartTimeMs: k.Window.Start(),
					EndTimeMs:   k.Window.End(),
				}
				newVal := ntypes.AuctionIdCount{
					AucId: k.Key.(uint64),
					Count: v,
				}
				return newKey, newVal, nil
			})))).
		Via(processor.NewGroupByOutputProcessor(sinks[0], &ectx))

	task := stream_task.NewStreamTaskBuilder().Build()
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		WindowStoreChangelogs(wsc).Build()
	return stream_task.ExecuteApp(ctx, task, streamTaskArgs)
}
