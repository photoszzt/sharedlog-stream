package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/commtypes"
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
) (*source_sink.MeteredSource, *source_sink.ConcurrentMeteredSink, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, err
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.StringSerde{},
		ValSerde: eventSerde,
		MsgSerde: msgSerde,
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}

	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: eventSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(true)
	sink := source_sink.NewConcurrentMeteredSink(source_sink.NewShardedSharedLogStreamSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

func (h *windowAvgGroupBy) process(ctx context.Context, sp *common.QueryInput,
	src *source_sink.MeteredSource, sink *source_sink.ConcurrentMeteredSink,
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
			if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success: true,
					Message: err.Error(),
					Latencies: map[string][]int{"e2e": latencies,
						"sink": sink.GetLatency(),
						"src":  src.GetLatency(),
					},
					Duration: time.Since(startTime).Seconds(),
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
			"e2e":  latencies,
			"sink": sink.GetLatency(),
			"src":  src.GetLatency(),
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
