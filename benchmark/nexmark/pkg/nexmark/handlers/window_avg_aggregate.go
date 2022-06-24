package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/store_with_changelog"
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
	*producer_consumer.MeteredConsumer, *producer_consumer.ConcurrentMeteredSink, error,
) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	eventSerde, err := ntypes.GetEventSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get evnet serde error: %v", err)
	}
	var wkSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		wkSerde = commtypes.WindowedKeyJSONSerde{
			KeyJSONSerde:    commtypes.Uint64Serde{},
			WindowJSONSerde: processor.TimeWindowJSONSerde{},
		}
	} else if serdeFormat == commtypes.MSGP {
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
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout:     common.SrcConsumeTimeout,
		KVMsgSerdes: kvmsgSerdes,
	}

	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			MsgSerde: msgSerde,
			KeySerde: wkSerde,
			ValSerde: commtypes.Float64Serde{},
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(false)
	sink := producer_consumer.NewConcurrentMeteredSyncProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_streams[0], outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink.MarkFinalOutput()
	return src, sink, nil
}

func (h *windowedAvg) getAggProcessor(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde) (*processor.MeteredProcessor, error) {
	var scSerde commtypes.Serde
	var vtSerde commtypes.Serde
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
	kvMsgSerdes := commtypes.KVMsgSerdes{
		MsgSerde: msgSerde,
		KeySerde: commtypes.Uint64Serde{},
		ValSerde: vtSerde,
	}
	tabName := "windowed-avg-store"
	winStoreMp, err := store_with_changelog.NewMaterializeParamBuilder().
		KVMsgSerdes(kvMsgSerdes).
		StoreName(tabName).
		ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		StreamParam(commtypes.CreateStreamParam{
			Env:          h.env,
			NumPartition: sp.NumInPartition,
		}).
		BuildForKVStore(time.Duration(sp.FlushMs)*time.Millisecond, common.SrcConsumeTimeout)
	if err != nil {
		return nil, err
	}

	store := store_with_changelog.NewInMemoryWindowStoreWithChangelog(
		timeWindows, false, concurrent_skiplist.CompareFunc(concurrent_skiplist.Uint64KeyCompare), winStoreMp)
	aggProc := processor.NewMeteredProcessor(processor.NewStreamWindowAggregateProcessor(
		"aggProc", store,
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
		}), timeWindows))
	return aggProc, nil
}

func (h *windowedAvg) process(ctx context.Context, sp *common.QueryInput,
	src *producer_consumer.MeteredConsumer, sink *producer_consumer.ConcurrentMeteredSink,
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
			if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success: true,
					Message: err.Error(),
					Latencies: map[string][]int{
						"e2e": latencies,
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
			"e2e": latencies,
		},
	}
}

func (h *windowedAvg) procMsg(ctx context.Context,
	msg commtypes.Message,
	aggProc *processor.MeteredProcessor,
	calcAvg *processor.MeteredProcessor,
	sink *producer_consumer.ConcurrentMeteredSink,
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
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
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
		})))
	return h.process(ctx, sp, src, sink, aggProc, calcAvg)
}
