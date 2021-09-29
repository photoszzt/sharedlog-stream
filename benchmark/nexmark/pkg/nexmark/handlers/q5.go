package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream"
	"sharedlog-stream/pkg/stream/processor"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query5Handler struct {
	env types.Environment
}

func NewQuery5(env types.Environment) types.FuncHandler {
	return &query5Handler{
		env: env,
	}
}

func (h *query5Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &ntypes.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	outputCh := make(chan *common.FnOutput)
	go Query5(ctx, h.env, parsedInput, outputCh)
	output := <-outputCh
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	fmt.Printf("query 2 output: %v\n", encodedOutput)
	return utils.CompressData(encodedOutput), nil
}

func Query5(ctx context.Context, env types.Environment, input *ntypes.QueryInput, output chan *common.FnOutput) {
	inputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.InputTopicName)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}

	outputStream, err := sharedlog_stream.NewSharedLogStream(ctx, env, input.OutputTopicName)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
		}
		return
	}

	windowChangeLog, err := sharedlog_stream.NewLogStore(ctx, env, "count-log")
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
		return
	}

	msgSerde, err := processor.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	var seSerde processor.Serde
	var aucIdCountSerde processor.Serde
	var aucIdCntMaxSerde processor.Serde
	if input.SerdeFormat == uint8(processor.JSON) {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
		aucIdCntMaxSerde = ntypes.AuctionIdCntMaxJSONSerde{}
	} else if input.SerdeFormat == uint8(processor.MSGP) {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
		aucIdCntMaxSerde = ntypes.AuctionIdCntMaxMsgpSerde{}
	} else {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   processor.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	builder := stream.NewStreamBuilder()
	inputs := builder.Source("nexmark-src", sharedlog_stream.NewSharedLogStreamSource(inputStream, inConfig))
	bid := inputs.Filter("filter-bid", processor.PredicateFunc(func(msg *processor.Message) (bool, error) {
		event := msg.Value.(*ntypes.Event)
		return event.Etype == ntypes.BID, nil
	})).Map("select-key", processor.MapperFunc(func(msg processor.Message) (processor.Message, error) {
		event := msg.Value.(*ntypes.Event)
		return processor.Message{Key: event.Bid.Auction, Value: msg.Value, Timestamp: msg.Timestamp}, nil
	}))
	auctionBids := bid.
		GroupByKey(&stream.Grouped{KeySerde: processor.Uint64Serde{}, Name: "group-by-auction-id"}).
		WindowedBy(processor.NewTimeWindowsNoGrace(time.Duration(10)*time.Second).AdvanceBy(time.Duration(2)*time.Second)).
		Count("count", &processor.MaterializeParam{
			KeySerde:   processor.Uint64Serde{},
			ValueSerde: processor.Uint64Serde{},
			MsgSerde:   msgSerde,
			StoreName:  "q5-count-store",
			Changelog:  windowChangeLog,
		}).
		ToStream().
		Map("change-key", processor.MapperFunc(func(msg processor.Message) (processor.Message, error) {
			key := msg.Key.(*stream.WindowedKey)
			value := msg.Value.(uint64)
			newKey := &ntypes.StartEndTime{
				StartTime: key.Window.Start(),
				EndTime:   key.Window.End(),
			}
			newVal := &ntypes.AuctionIdCount{
				AucId: key.Key.(uint64),
				Count: value,
			}
			return processor.Message{Key: newKey, Value: newVal, Timestamp: msg.Timestamp}, nil
		}))

	maxBids := auctionBids.
		GroupByKey(&stream.Grouped{KeySerde: seSerde, ValueSerde: aucIdCountSerde, Name: "auctionbids-groupbykey"}).
		Aggregate("aggregate",
			&processor.MaterializeParam{KeySerde: seSerde, ValueSerde: aucIdCountSerde, StoreName: "agg-store"},
			processor.InitializerFunc(func() interface{} { return 0 }),
			processor.AggregatorFunc(func(key interface{}, value interface{}, aggregate interface{}) interface{} {
				v := value.(*ntypes.AuctionIdCount)
				agg := aggregate.(uint64)
				if v.Count > agg {
					return v.Count
				} else {
					return agg
				}
			}))

	auctionBids.
		StreamTableJoin("join-auctionbids-maxbids",
			maxBids,
			processor.ValueJoinerWithKeyFunc(func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
				lv := leftValue.(*ntypes.AuctionIdCount)
				rv := rightValue.(uint64)
				return &ntypes.AuctionIdCntMax{
					AucId:  lv.AucId,
					Count:  lv.Count,
					MaxCnt: rv,
				}
			})).
		Filter("choose-maxcnt", processor.PredicateFunc(func(msg *processor.Message) (bool, error) {
			v := msg.Value.(*ntypes.AuctionIdCntMax)
			return v.Count >= v.MaxCnt, nil
		})).
		Process("sink", sharedlog_stream.NewSharedLogStreamSink(outputStream, seSerde, aucIdCntMaxSerde, msgSerde))
	tp, err_arrs := builder.Build()
	if err_arrs != nil {
		output <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("build stream failed: %v", err_arrs),
		}
	}
	pumps := make(map[processor.Node]processor.Pump)
	var srcPumps []processor.SourcePump
	nodes := processor.FlattenNodeTree(tp.Sources())
	processor.ReverseNodes(nodes)
	for _, node := range nodes {
		pipe := processor.NewPipe(processor.ResolvePumps(pumps, node.Children()))
		node.Processor().WithPipe(pipe)

		pump := processor.NewSyncPump(node, pipe)
		pumps[node] = pump
	}
	for source, node := range tp.Sources() {
		srcPump := processor.NewSourcePump(node.Name(), source,
			processor.ResolvePumps(pumps, node.Children()), func(err error) {
				log.Fatal(err.Error())
			})
		srcPumps = append(srcPumps, srcPump)
	}

	duration := time.Duration(input.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	select {
	case <-time.After(duration):
		for _, srcPump := range srcPumps {
			srcPump.Stop()
			srcPump.Close()
		}
	}
	output <- &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: latencies,
	}
}
