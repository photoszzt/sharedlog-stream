package wordcount

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"sharedlog-stream/pkg/stream/processor/store_with_changelog"
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
	mp := &store_with_changelog.MaterializeParam{
		KeySerde:   commtypes.StringSerde{},
		ValueSerde: vtSerde,
		MsgSerde:   msgSerde,
		StoreName:  sp.OutputTopicNames[0],
		Changelog:  output_stream,
		ParNum:     sp.ParNum,
	}
	inMemStore := store.NewInMemoryKeyValueStore(mp.StoreName, func(a, b treemap.Key) int {
		ka := a.(string)
		kb := b.(string)
		return strings.Compare(ka, kb)
	})
	store := store_with_changelog.NewKeyValueStoreWithChangelog(mp, inMemStore, false)
	// fmt.Fprintf(os.Stderr, "before restore\n")
	p := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}),
		processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			aggVal := agg.(uint64)
			fmt.Fprintf(os.Stderr, "update %v count to %d\n", key, aggVal+1)
			return aggVal + 1
		})), time.Duration(0))
	return p, nil
}

type wordcountCounterAggProcessArg struct {
	src              *processor.MeteredSource
	output_stream    *store.MeteredStream
	counter          *processor.MeteredProcessor
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func (a *wordcountCounterAggProcessArg) ParNum() uint8    { return a.parNum }
func (a *wordcountCounterAggProcessArg) CurEpoch() uint64 { return a.curEpoch }
func (a *wordcountCounterAggProcessArg) FuncName() string { return a.funcName }
func (a *wordcountCounterAggProcessArg) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *wordcountCounterAgg) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*wordcountCounterAggProcessArg)
	msgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			return t.CurrentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return t.CurrentOffset, &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("consume failed: %v", err),
		}
	}

	for _, msg := range msgs.Msgs {
		if msg.Msg.Value == nil {
			continue
		}
		if msg.IsControl {
			v := msg.Msg.Value.(sharedlog_stream.ScaleEpochAndBytes)
			_, err = args.output_stream.Push(ctx, v.Payload, args.parNum, true, false)
			if err != nil {
				return t.CurrentOffset, &common.FnOutput{Success: false, Message: err.Error()}
			}
			if args.curEpoch < v.ScaleEpoch {
				err = args.recordFinishFunc(ctx, args.funcName, args.parNum)
				if err != nil {
					return t.CurrentOffset, &common.FnOutput{Success: false, Message: err.Error()}
				}
				return t.CurrentOffset, &common.FnOutput{
					Success: true,
					Message: fmt.Sprintf("%s-%d epoch %d exit", args.funcName, args.parNum, args.curEpoch),
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
					return t.CurrentOffset, &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("counter failed: %v", err),
					}
				}
			}
		} else {
			_, err = args.counter.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return t.CurrentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("counter failed: %v", err),
				}
			}
		}
	}
	return t.CurrentOffset, nil
}

func (h *wordcountCounterAgg) wordcount_counter(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, true)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get input output stream failed: %v", err),
		}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	meteredOutputStream := store.NewMeteredStream(output_streams[0])

	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get msg serde failed: %v", err),
		}
	}
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: commtypes.StringDecoder{},
		MsgDecoder:   msgSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	count, err := setupCounter(ctx, sp, msgSerde, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("setup counter failed: %v", err),
		}
	}

	funcName := "wccounter"
	procArgs := &wordcountCounterAggProcessArg{
		src:              src,
		output_stream:    meteredOutputStream,
		counter:          count,
		parNum:           sp.ParNum,
		funcName:         funcName,
		curEpoch:         sp.ScaleEpoch,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		trackParFunc:     transaction.DefaultTrackSubstreamFunc,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}

	if sp.EnableTransaction {
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			MsgSerde:        msgSerde,
			Srcs:            srcs,
			OutputStreams:   output_streams,
			QueryInput:      sp,
			TransactionalId: fmt.Sprintf("%s-%s-%s-%d", funcName, sp.InputTopicNames[0], sp.OutputTopicNames[0], sp.ParNum),
			FixedOutParNum:  sp.ParNum,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*wordcountCounterAggProcessArg).trackParFunc = trackParFunc
				procArgs.(*wordcountCounterAggProcessArg).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["count"] = count.GetLatency()
			ret.Latencies["changelogRead"] = meteredOutputStream.GetReadNextLatencies()
			ret.Latencies["changelogPush"] = meteredOutputStream.GetPushLatencies()
		}
		return ret
	}
	// return h.process(ctx, sp, args)
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs:   procArgs,
		Duration:   time.Duration(sp.Duration) * time.Second,
		WarmupTime: time.Duration(sp.WarmupS) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["count"] = count.GetLatency()
		ret.Latencies["changelogRead"] = meteredOutputStream.GetReadNextLatencies()
		ret.Latencies["changelogPush"] = meteredOutputStream.GetPushLatencies()
	}
	return ret
}
