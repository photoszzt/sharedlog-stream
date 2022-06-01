package wordcount

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/transaction"
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
	src           *source_sink.MeteredSource
	output_stream *store.MeteredStream
	counter       *processor.MeteredProcessor
	proc_interface.BaseProcArgs
}

func (h *wordcountCounterAgg) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	args := argsTmp.(*wordcountCounterAggProcessArg)
	msgs, err := args.src.Consume(ctx, args.ParNum())
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
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
			_, err = args.output_stream.Push(ctx, v.Payload, args.ParNum(), true, false, 0, 0, 0)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			if args.CurEpoch() < v.ScaleEpoch {
				err = args.RecordFinishFunc()(ctx, args.FuncName(), args.ParNum())
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				return &common.FnOutput{
					Success: true,
					Message: fmt.Sprintf("%s-%d epoch %d exit", args.FuncName(), args.ParNum(), args.CurEpoch()),
					Err:     errors.ErrShouldExitForScale,
				}
			}
			continue
		}
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
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
	meteredOutputStream := store.NewMeteredStream(output_streams[0])
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
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
		time.Duration(sp.WarmupS)*time.Second)
	src.SetInitialSource(false)
	count, err := setupCounter(ctx, sp, msgSerde, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("setup counter failed: %v", err),
		}
	}

	funcName := "wccounter"
	procArgs := &wordcountCounterAggProcessArg{
		src:           src,
		output_stream: meteredOutputStream,
		counter:       count,
		BaseProcArgs:  proc_interface.NewBaseProcArgs(funcName, sp.ScaleEpoch, sp.ParNum),
	}

	task := transaction.NewStreamTaskBuilder().
		AppProcessFunc(h.process).
		InitFunc(func(progArgs interface{}) {
			src.StartWarmup()
			count.StartWarmup()
		}).Build()

	srcs := []source_sink.Source{src}
	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["count"] = count.GetLatency()
		ret.Latencies["changelogRead"] = meteredOutputStream.GetReadNextLatencies()
		ret.Latencies["changelogPush"] = meteredOutputStream.GetPushLatencies()
	}
	if sp.EnableTransaction {
		transactionalID := fmt.Sprintf("%s-%s-%s-%d", funcName, sp.InputTopicNames[0], sp.OutputTopicNames[0], sp.ParNum)
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, nil)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs, task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, nil)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
}
