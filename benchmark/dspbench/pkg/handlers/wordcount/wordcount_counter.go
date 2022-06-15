package wordcount

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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/stream_task"
	"sharedlog-stream/pkg/treemap"
	"strings"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type wordcountCounterAgg struct {
	env types.Environment
}

func NewWordCountCounterAgg(env types.Environment) *wordcountCounterAgg {
	return &wordcountCounterAgg{
		env: env,
	}
}

func (h *wordcountCounterAgg) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.wordcount_counter(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func setupCounter(ctx context.Context, sp *common.QueryInput, msgSerde commtypes.MsgSerde,
	output_stream *sharedlog_stream.ShardedSharedLogStream) (*processor.MeteredProcessor, error) {
	var vtSerde commtypes.Serde
	if sp.SerdeFormat == uint8(commtypes.JSON) {
		vtSerde = commtypes.ValueTimestampJSONSerde{
			ValJSONSerde: commtypes.Uint64Serde{},
		}
	} else if sp.SerdeFormat == uint8(commtypes.MSGP) {
		vtSerde = commtypes.ValueTimestampMsgpSerde{
			ValMsgpSerde: commtypes.Uint64Serde{},
		}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat)
	}
	kvMsgSerdes := commtypes.KVMsgSerdes{
		KeySerde: commtypes.StringSerde{},
		ValSerde: vtSerde,
		MsgSerde: msgSerde,
	}
	mp, err := store_with_changelog.NewMaterializeParamBuilder().KVMsgSerdes(kvMsgSerdes).
		StoreName(sp.OutputTopicNames[0] + "-tab").ParNum(sp.ParNum).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		ChangelogManager(store_with_changelog.NewChangelogManager(output_stream, commtypes.SerdeFormat(sp.SerdeFormat))).Build()
	if err != nil {
		return nil, err
	}
	compare := func(a, b treemap.Key) int {
		ka := a.(string)
		kb := b.(string)
		return strings.Compare(ka, kb)
	}
	warmup := time.Duration(sp.WarmupS) * time.Second
	store := store_with_changelog.CreateInMemKVTableWithChangelog(mp, compare, warmup)
	// fmt.Fprintf(os.Stderr, "before restore\n")
	p := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}),
		processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			aggVal := agg.(uint64)
			fmt.Fprintf(os.Stderr, "update %v count to %d\n", key, aggVal+1)
			return aggVal + 1
		})), warmup)
	return p, nil
}

type wordcountCounterAggProcessArg struct {
	counter *processor.MeteredProcessor
	proc_interface.BaseExecutionContext
}

func (h *wordcountCounterAgg) process(ctx context.Context,
	t *stream_task.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	args := argsTmp.(*wordcountCounterAggProcessArg)
	msgs, err := args.Sources()[0].Consume(ctx, args.SubstreamNum())
	if err != nil {
		if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
			return &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("consume failed: %v", err),
		}
	}

	for _, msg := range msgs.Msgs {
		if msg.Msg.Value == nil {
			continue
		}
		if msg.IsControl {
			v := msg.Msg.Value.(source_sink.ScaleEpochAndBytes)
			// TODO: below is not correct
			err = args.Sinks()[0].Produce(ctx, msg.Msg, args.SubstreamNum(), true)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			if args.CurEpoch() < v.ScaleEpoch {
				err = args.RecordFinishFunc()(ctx, args.FuncName(), args.SubstreamNum())
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				return &common.FnOutput{
					Success: true,
					Message: fmt.Sprintf("%s-%d epoch %d exit", args.FuncName(), args.SubstreamNum(), args.CurEpoch()),
					Err:     common_errors.ErrShouldExitForScale,
				}
			}
			continue
		}
		t.CurrentOffset[args.Sources()[0].TopicName()] = msg.LogSeqNum
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				_, err = args.counter.ProcessAndReturn(ctx, subMsg)
				if err != nil {
					return &common.FnOutput{Success: false, Message: fmt.Sprintf("counter failed: %v", err)}
				}
			}
		} else {
			_, err = args.counter.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return &common.FnOutput{Success: false, Message: fmt.Sprintf("counter failed: %v", err)}
			}
		}
	}
	return nil
}

func (h *wordcountCounterAgg) wordcount_counter(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	warmup := time.Duration(sp.WarmupS) * time.Second
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get msg serde failed: %v", err),
		}
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: commtypes.StringSerde{},
			MsgSerde: msgSerde,
		},
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		warmup)
	sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_streams[0], &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: commtypes.Uint64Serde{},
			MsgSerde: msgSerde,
		},
	}), warmup)
	src.SetInitialSource(false)
	sink.MarkFinalOutput()
	count, err := setupCounter(ctx, sp, msgSerde, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("setup counter failed: %v", err),
		}
	}

	funcName := "wccounter"
	srcs := []source_sink.MeteredSourceIntr{src}
	procArgs := &wordcountCounterAggProcessArg{
		counter: count,
		BaseExecutionContext: proc_interface.NewExecutionContext(srcs, []source_sink.MeteredSink{sink},
			funcName, sp.ScaleEpoch, sp.ParNum),
	}

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(h.process).
		InitFunc(func(progArgs interface{}) {
			count.StartWarmup()
		}).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["count"] = count.GetLatency()
	}
	transactionalID := fmt.Sprintf("%s-%s-%s-%d", funcName, sp.InputTopicNames[0], sp.OutputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, sp.EnableTransaction, update_stats)
}
