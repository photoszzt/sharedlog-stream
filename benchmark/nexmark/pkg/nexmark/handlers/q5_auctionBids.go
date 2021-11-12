package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type q5AuctionBids struct {
	env types.Environment
}

func hashSe(key *ntypes.StartEndTime) uint32 {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%v", key)))
	return h.Sum32()
}

func NewQ5AuctionBids(env types.Environment) *q5AuctionBids {
	return &q5AuctionBids{
		env: env,
	}
}

func (h *q5AuctionBids) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.process(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *q5AuctionBids) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := getShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	var seSerde commtypes.Serde
	var aucIdCountSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		seSerde = ntypes.StartEndTimeJSONSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountJSONSerde{}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		seSerde = ntypes.StartEndTimeMsgpSerde{}
		aucIdCountSerde = ntypes.AuctionIdCountMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}

	duration := time.Duration(sp.Duration) * time.Second
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      duration,
		KeyDecoder:   commtypes.Uint64Serde{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgEncoder:   msgSerde,
		KeyEncoder:   seSerde,
		ValueEncoder: aucIdCountSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))

	hopWindow := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second).AdvanceBy(time.Duration(2) * time.Second)
	countStoreName := "auctionBidsCountStore"
	changelogName := countStoreName + "-changelog"
	changelog, err := sharedlog_stream.NewShardedSharedLogStream(h.env, changelogName, uint8(sp.NumOutPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
		}
	}
	countMp := &store.MaterializeParam{
		KeySerde:   commtypes.Uint64Serde{},
		ValueSerde: commtypes.Uint64Serde{},
		MsgSerde:   msgSerde,
		StoreName:  countStoreName,
		Changelog:  changelog,
	}
	countWindowStore := store.NewInMemoryWindowStoreWithChangelog(
		hopWindow.MaxSize()+hopWindow.GracePeriodMs(),
		hopWindow.MaxSize(), countMp,
	)
	countProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(countWindowStore,
		processor.InitializerFunc(func() interface{} { return 0 }),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := aggregate.(uint64)
			return val + 1
		}), hopWindow))

	groupByAuction := processor.NewMeteredProcessor(processor.NewStreamMapProcessor(processor.MapperFunc(
		func(msg commtypes.Message) (commtypes.Message, error) {
			key := msg.Key.(*commtypes.WindowedKey)
			value := msg.Value.(uint64)
			newKey := &ntypes.StartEndTime{
				StartTime: key.Window.Start(),
				EndTime:   key.Window.End(),
			}
			newVal := &ntypes.AuctionIdCount{
				AucId: key.Key.(uint64),
				Count: value,
			}
			return commtypes.Message{Key: newKey, Value: newVal, Timestamp: msg.Timestamp}, nil
		})))
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msgs, err := src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success:  true,
					Message:  err.Error(),
					Duration: time.Since(startTime).Seconds(),
					Latencies: map[string][]int{
						"e2e":       latencies,
						"count":     countProc.GetLatency(),
						"changeKey": groupByAuction.GetLatency(),
						"src":       src.GetLatency(),
						"sink":      sink.GetLatency(),
					},
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		for _, msg := range msgs {
			countMsgs, err := countProc.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			for _, countMsg := range countMsgs {
				changeKeyedMsg, err := groupByAuction.ProcessAndReturn(ctx, countMsg)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
				par := uint8(hashSe(changeKeyedMsg[0].Key.(*ntypes.StartEndTime)) % uint32(sp.NumOutPartition))
				err = sink.Sink(ctx, changeKeyedMsg[0], par, false)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: err.Error(),
					}
				}
			}
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":       latencies,
			"count":     countProc.GetLatency(),
			"changeKey": groupByAuction.GetLatency(),
			"src":       src.GetLatency(),
			"sink":      sink.GetLatency(),
		},
	}
}
