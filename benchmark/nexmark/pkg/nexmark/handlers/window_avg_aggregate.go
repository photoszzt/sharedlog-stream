package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store_with_changelog"
	"time"

	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

// groupBy(bid.Auction).
// windowBy(TimeWindow(10 s)).
// aggregate(bid.price)

type windowedAvg struct {
	env types.Environment
}

func NewWindowedAvg(env types.Environment) types.FuncHandler {
	return &windowedAvg{
		env: env,
	}
}

func (h *windowedAvg) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		fmt.Fprintf(os.Stderr, "unmarshal error")
		return nil, err
	}
	output := h.windowavg_aggregate(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "marshal error")
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *windowedAvg) getSrcSink(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde) (
	*source_sink.MeteredSource, *source_sink.ConcurrentMeteredSyncSink, error,
) {
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get evnet serde error: %v", err)
	}
	var wkSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		wkSerde = commtypes.WindowedKeyJSONSerde{
			KeyJSONSerde:    commtypes.Uint64Serde{},
			WindowJSONSerde: processor.TimeWindowJSONSerde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		wkSerde = commtypes.WindowedKeyMsgpSerde{
			KeyMsgpSerde:    commtypes.Uint64Serde{},
			WindowMsgpSerde: processor.TimeWindowMsgpSerde{},
		}
	} else {
		return nil, nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return nil, nil, err
	}
	kvmsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.Uint64Serde{},
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
			KeySerde: wkSerde,
			ValSerde: commtypes.Float64Serde{},
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(false)
	sink := source_sink.NewConcurrentMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_streams[0], outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink.MarkFinalOutput()
	return src, sink, nil
}

func (h *windowedAvg) getAggProcessor(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde) (*processor.MeteredProcessor, error) {
	var scSerde commtypes.Serde
	var vtSerde commtypes.Serde
	changelog_stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, "windowedAvgAgg_changelog", uint8(sp.NumOutPartitions[0]), commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return nil, fmt.Errorf("create changelog stream failed: %v", err)
	}
	/*
		err = changelog_stream.InitStream(ctx, true)
		if err != nil {
			return nil, fmt.Errorf("changelog stream init failed: %v", err)
		}
	*/
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		scSerde = ntypes.SumAndCountJSONSerde{}
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: scSerde,
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		scSerde = ntypes.SumAndCountMsgpSerde{}
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: scSerde,
		}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}

	timeWindows, err := processor.NewTimeWindowsNoGrace(time.Duration(10) * time.Second)
	if err != nil {
		return nil, err
	}
	winStoreMp := &store_with_changelog.MaterializeParam{
		StoreName: "windowed-avg-store",
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: commtypes.Uint64Serde{},
			ValSerde: vtSerde,
		},
		ChangelogManager: store_with_changelog.NewChangelogManager(changelog_stream, commtypes.SerdeFormat(sp.SerdeFormat)),
		ParNum:           sp.ParNum,
	}

	store, err := store_with_changelog.NewInMemoryWindowStoreWithChangelog(
		timeWindows.MaxSize()+timeWindows.GracePeriodMs(), timeWindows.MaxSize(), false, winStoreMp)
	if err != nil {
		return nil, err
	}
	aggProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return &ntypes.SumAndCount{
				Sum:   0,
				Count: 0,
			}
		}),
		processor.AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
			val := value.(*ntypes.Event)
			agg := aggregate.(*ntypes.SumAndCount)
			return &ntypes.SumAndCount{
				Sum:   agg.Sum + val.Bid.Price,
				Count: agg.Count + 1,
			}
		}), timeWindows), time.Duration(sp.WarmupS)*time.Second)
	return aggProc, nil
}

func (h *windowedAvg) process(ctx context.Context, sp *common.QueryInput,
	src *source_sink.MeteredSource, sink *source_sink.ConcurrentMeteredSyncSink,
	aggProc *processor.MeteredProcessor, calcAvg *processor.MeteredProcessor,
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
					Latencies: map[string][]int{
						"e2e":     latencies,
						"agg":     aggProc.GetLatency(),
						"calcAvg": calcAvg.GetLatency(),
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
					err = h.procMsg(ctx, subMsg, aggProc, calcAvg, sink, sp.ParNum)
					if err != nil {
						return &common.FnOutput{Success: false, Message: err.Error()}
					}
				}
			} else {
				if msg.Msg.Value == nil {
					continue
				}
				err = h.procMsg(ctx, msg.Msg, aggProc, calcAvg, sink, sp.ParNum)
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
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
			"e2e":     latencies,
			"agg":     aggProc.GetLatency(),
			"calcAvg": calcAvg.GetLatency(),
		},
	}
}

func (h *windowedAvg) procMsg(ctx context.Context,
	msg commtypes.Message,
	aggProc *processor.MeteredProcessor,
	calcAvg *processor.MeteredProcessor,
	sink *source_sink.ConcurrentMeteredSyncSink,
	parNum uint8,
) error {
	newMsgs, err := aggProc.ProcessAndReturn(ctx, msg)
	if err != nil {
		return fmt.Errorf("aggregate failed: %v\n", err)
	}
	for _, newMsg := range newMsgs {
		avg, err := calcAvg.ProcessAndReturn(ctx, newMsg)
		if err != nil {
			return fmt.Errorf("calculate avg failed: %v\n", err)

		}
		err = sink.Produce(ctx, avg[0], parNum, false)
		if err != nil {
			return fmt.Errorf("sink failed: %v", err)
		}
	}
	return nil
}

func (h *windowedAvg) windowavg_aggregate(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get msg serde failed: %v", err),
		}
	}
	src, sink, err := h.getSrcSink(ctx, sp, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	aggProc, err := h.getAggProcessor(ctx, sp, msgSerde)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	calcAvg := processor.NewMeteredProcessor(processor.NewStreamMapValuesProcessor(
		processor.ValueMapperFunc(func(value interface{}) (interface{}, error) {
			val := value.(*ntypes.SumAndCount)
			return float64(val.Sum) / float64(val.Count), nil
		})), time.Duration(sp.WarmupS)*time.Second)
	return h.process(ctx, sp, src, sink, aggProc, calcAvg)
}
