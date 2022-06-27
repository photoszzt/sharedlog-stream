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
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
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
	srcMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, eventSerde)
	if err != nil {
		return nil, nil, err
	}
	sinkMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, seSerde, aucIdCountSerde)
	if err != nil {
		return nil, nil, err
	}
	src := producer_consumer.NewMeteredConsumer(
		producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, &producer_consumer.StreamConsumerConfig{
			Timeout:  time.Duration(5) * time.Second,
			MsgSerde: srcMsgSerde,
		}), warmup)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(
		producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], &producer_consumer.StreamSinkConfig{
			MsgSerde:      sinkMsgSerde,
			FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
		}), warmup)
	src.SetInitialSource(false)
	return []producer_consumer.MeteredConsumerIntr{src}, []producer_consumer.MeteredProducerIntr{sink}, nil
}

func (h *q5AuctionBids) getCountAggProc(ctx context.Context, sp *common.QueryInput,
) (*processor.MeteredProcessor, []*store_restore.WindowStoreChangelog, error) {
	hopWindow, err := processor.NewTimeWindowsWithGrace(time.Duration(10)*time.Second, time.Duration(5)*time.Second)
	if err != nil {
		return nil, nil, err
	}
	hopWindow, err = hopWindow.AdvanceBy(time.Duration(2) * time.Second)
	if err != nil {
		return nil, nil, err
	}
	msgSerde, err := commtypes.GetMsgSerde(commtypes.SerdeFormat(sp.SerdeFormat),
		commtypes.Uint64Serde{}, commtypes.Uint64Serde{})
	if err != nil {
		return nil, nil, err
	}
	var countWindowStore store.WindowStore
	countStoreName := "auctionBidsCountStore"
	comparable := concurrent_skiplist.CompareFunc(concurrent_skiplist.Uint64KeyCompare)
	countMp, err := store_with_changelog.NewMaterializeParamBuilder().
		MessageSerde(msgSerde).StoreName(countStoreName).ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).BuildForWindowStore(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return nil, nil, err
	}
	countWindowStore = store_with_changelog.NewInMemoryWindowStoreWithChangelog(
		hopWindow, false, comparable, countMp)
	countProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(
		"countProc", countWindowStore,
		processor.InitializerFunc(func() interface{} { return uint64(0) }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow))
	wsc := []*store_restore.WindowStoreChangelog{
		store_restore.NewWindowStoreChangelog(
			countWindowStore, countMp.ChangelogManager(), sp.ParNum),
	}
	return countProc, wsc, nil
}

/*
type q5AuctionBidsProcessArg struct {
	countProc      *processor.MeteredProcessor
	groupByAuction *processor.MeteredProcessor
	groupBy        *processor.MeteredProcessor
	processor.BaseExecutionContext
}

type q5AuctionBidsRestoreArg struct {
	countProc      processor.Processor
	groupByAuction processor.Processor
	src            producer_consumer.Consumer
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
		_, err = args.groupBy.ProcessAndReturn(ctx, changeKeyedMsg[0])
		if err != nil {
			return err
		}
	}
	return nil
}
*/

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
		Via(processor.NewMeteredProcessor(processor.NewStreamMapProcessor("groupByAuction",
			processor.MapperFunc(
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
				})))).
		Via(processor.NewGroupByOutputProcessor(sinks[0], &ectx))

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(processor.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, processor.ProcessMsg)
		}).Build()
	transactionalID := fmt.Sprintf("%s-%s-%d-%s", h.funcName, sp.InputTopicNames[0],
		sp.ParNum, sp.OutputTopicNames[0])
	builder := stream_task.NewStreamTaskArgsBuilder(h.env, &ectx, transactionalID)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp, builder).
		WindowStoreChangelogs(wsc).Build()
	return task.ExecuteApp(ctx, streamTaskArgs)
}
