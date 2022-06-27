package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

// groupBy(bid.Auction).
// windowBy(TimeWindow(10 s)).
// aggregate(bid.price)

type windowAvgGroupBy struct {
	env types.Environment
}

func NewWindowAvgGroupByHandler(env types.Environment) types.FuncHandler {
	return &windowAvgGroupBy{
		env: env,
	}
}

func (h *windowAvgGroupBy) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.windowavg_groupby(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *windowAvgGroupBy) getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*producer_consumer.MeteredConsumer, *producer_consumer.ConcurrentMeteredSink, error) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, err
	}
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.StringSerde{}, eventSerde)
	if err != nil {
		return nil, nil, err
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:  common.SrcConsumeTimeout,
		MsgSerde: msgSerde,
	}
	outMsgSerde, err := commtypes.GetMsgSerde(serdeFormat, commtypes.Uint64Serde{}, eventSerde)
	if err != nil {
		return nil, nil, err
	}

	outConfig := &producer_consumer.StreamSinkConfig{
		MsgSerde:      outMsgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(true)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

func (h *windowAvgGroupBy) process(ctx context.Context, sp *common.QueryInput,
	src *producer_consumer.MeteredConsumer, sink *producer_consumer.ConcurrentMeteredSink,
) *common.FnOutput {
	duration := time.Duration(sp.Duration) * time.Second
	latencies := make([]int, 0, 128)
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msgs, err := src.Consume(ctx, sp.ParNum)
		if err != nil {
			if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success:   true,
					Message:   err.Error(),
					Latencies: map[string][]int{"e2e": latencies},
					Duration:  time.Since(startTime).Seconds(),
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}

		for _, msg := range msgs.Msgs {
			if msg.MsgArr != nil {
				for _, subMsg := range msg.MsgArr {
					if subMsg.Value == nil {
						continue
					}
					val := subMsg.Value.(*ntypes.Event)
					if val.Etype == ntypes.BID {
						par := uint8(val.Bid.Auction % uint64(sp.NumOutPartitions[0]))
						newMsg := commtypes.Message{Key: val.Bid.Auction, Value: msg.Msg.Value}
						err = sink.Produce(ctx, newMsg, par, false)
						if err != nil {
							return &common.FnOutput{
								Success: false,
								Message: fmt.Sprintf("sink failed: %v\n", err),
							}
						}
					}
				}
			} else {
				if msg.Msg.Value == nil {
					continue
				}
				val := msg.Msg.Value.(*ntypes.Event)
				if val.Etype == ntypes.BID {
					par := uint8(val.Bid.Auction % uint64(sp.NumOutPartitions[0]))
					newMsg := commtypes.Message{Key: val.Bid.Auction, Value: msg.Msg.Value}
					err = sink.Produce(ctx, newMsg, par, false)
					if err != nil {
						return &common.FnOutput{
							Success: false,
							Message: fmt.Sprintf("sink failed: %v\n", err),
						}
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
			"e2e": latencies,
		},
	}
}

func (h *windowAvgGroupBy) windowavg_groupby(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src, sink, err := h.getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	return h.process(ctx, sp, src, sink)
}
