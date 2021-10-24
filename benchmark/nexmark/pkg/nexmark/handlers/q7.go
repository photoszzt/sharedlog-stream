package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
)

type query7Handler struct {
	env types.Environment
}

func NewQuery7(env types.Environment) types.FuncHandler {
	return &query7Handler{
		env: env,
	}
}

func (h *query7Handler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.QueryInput{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.process(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *query7Handler) process(ctx context.Context, input *common.QueryInput) *common.FnOutput {
	inputStream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, input.InputTopicName, uint8(input.NumInPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for input stream failed: %v", err),
		}
	}

	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewSharedlogStream for output stream failed: %v", err),
		}
	}

	msgSerde, err := commtypes.GetMsgSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(input.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	var ptSerde commtypes.Serde
	if input.SerdeFormat == uint8(commtypes.JSON) {
		ptSerde = ntypes.PriceTimeJSONSerde{}
	} else if input.SerdeFormat == uint8(commtypes.MSGP) {
		ptSerde = ntypes.PriceTimeMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", input.SerdeFormat),
		}
	}

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(input.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder: msgSerde,
	}
	duration := time.Duration(input.Duration) * time.Second
	src := sharedlog_stream.NewShardedSharedLogStreamSource(inputStream, inConfig)
	sink := sharedlog_stream.NewShardedSharedLogStreamSink(outputStream, outConfig)

	tw := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second)
	maxPriceBidStoreName := "max-price-bid-tab"
	mp := &store.MaterializeParam{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: ptSerde,
		StoreName:  maxPriceBidStoreName,
		ParNum:     input.ParNum,
		Changelog:  outputStream,
		MsgSerde:   msgSerde,
	}
	store := store.NewInMemoryWindowStoreWithChangelog(tw.MaxSize()+tw.GracePeriodMs(), tw.MaxSize(), mp)

	maxPriceBid := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return ntypes.PriceTime{
				Price:    0,
				DateTime: 0,
			}
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := value.(*ntypes.Event)
			agg := aggregate.(ntypes.PriceTime)
			if val.Bid.Price > agg.Price {
				return ntypes.PriceTime{
					Price:    val.Bid.Price,
					DateTime: val.Bid.DateTime,
				}
			} else {
				return agg
			}

		}), tw))
	transformWithStore := processor.NewMeteredProcessor(NewQ7TransformProcessor(store))
	filterTime := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor(processor.PredicateFunc(func(m *commtypes.Message) (bool, error) {
		bm := m.Value.(ntypes.BidAndMax)
		lb := bm.MaxDateTime - 10*1000
		return bm.DateTime >= lb && bm.DateTime <= bm.MaxDateTime, nil
	})))

	srcLatencies := make([]int, 0, 128)
	sinkLatencies := make([]int, 0, 128)

	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msg, err := src.Consume(input.ParNum)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		srcLat := time.Since(procStart)
		srcLatencies = append(srcLatencies, int(srcLat.Microseconds()))
		maxPriceBidMsg, err := maxPriceBid.ProcessAndReturn(msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		transformedMsgs, err := transformWithStore.ProcessAndReturn(maxPriceBidMsg[0])
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, tmsg := range transformedMsgs {
			filtered, err := filterTime.ProcessAndReturn(tmsg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			sinkStart := time.Now()
			err = sink.Sink(filtered[0], input.ParNum)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			sinkLat := time.Since(sinkStart)
			sinkLatencies = append(sinkLatencies, int(sinkLat.Microseconds()))
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}

	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":    latencies,
			"sink":   sinkLatencies,
			"src":    srcLatencies,
			"maxBid": maxPriceBid.GetLatency(),
			"trans":  transformWithStore.GetLatency(),
			"filtT":  filterTime.GetLatency(),
		},
	}
}
