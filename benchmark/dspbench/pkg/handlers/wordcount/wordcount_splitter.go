package wordcount

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"regexp"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"strings"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type wordcountSplitFlatMap struct {
	env types.Environment
}

func NewWordCountSplitter(env types.Environment) *wordcountSplitFlatMap {
	return &wordcountSplitFlatMap{
		env: env,
	}
}

func (h *wordcountSplitFlatMap) Call(ctx context.Context, input []byte) ([]byte, error) {
	sp := &common.QueryInput{}
	err := json.Unmarshal(input, sp)
	if err != nil {
		return nil, err
	}
	output := h.wordcount_split(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func getSrcSink(ctx context.Context,
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource,
	*processor.MeteredSink,
	commtypes.MsgSerde,
	error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: commtypes.StringDecoder{},
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.StringSerde{},
		ValueSerde: commtypes.StringSerde{},
		MsgSerde:   msgSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, msgSerde, nil
}

type wordcountSplitterProcessArg struct {
	src              *processor.MeteredSource
	sink             *processor.MeteredSink
	output_stream    *sharedlog_stream.ShardedSharedLogStream
	splitter         processor.FlatMapperFunc
	trackParFunc     transaction.TrackKeySubStreamFunc
	recordFinishFunc transaction.RecordPrevInstanceFinishFunc
	funcName         string
	splitLatencies   []int
	curEpoch         uint64
	parNum           uint8
	numOutPartition  uint8
}

func (a *wordcountSplitterProcessArg) Source() processor.Source { return a.src }
func (a *wordcountSplitterProcessArg) Sink() processor.Sink     { return a.sink }
func (a *wordcountSplitterProcessArg) ParNum() uint8            { return a.parNum }
func (a *wordcountSplitterProcessArg) CurEpoch() uint64         { return a.curEpoch }
func (a *wordcountSplitterProcessArg) FuncName() string         { return a.funcName }
func (a *wordcountSplitterProcessArg) RecordFinishFunc() func(ctx context.Context, funcName string, instanceId uint8) error {
	return a.recordFinishFunc
}

func (h *wordcountSplitFlatMap) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) (map[string]uint64, *common.FnOutput) {
	args := argsTmp.(*wordcountSplitterProcessArg)
	return transaction.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		splitStart := time.Now()
		msgs, err := args.splitter(msg.Msg)
		if err != nil {
			return fmt.Errorf("splitter failed: %v\n", err)
		}
		splitLat := time.Since(splitStart)
		args.splitLatencies = append(args.splitLatencies, int(splitLat.Microseconds()))
		for _, m := range msgs {
			hashed := hashKey(m.Key.(string))
			par := uint8(hashed % uint32(args.numOutPartition))
			err = args.trackParFunc(ctx, m.Key, args.sink.KeySerde(), args.sink.TopicName(), par)
			if err != nil {
				return fmt.Errorf("add topic partition failed: %v\n", err)
			}
			err = args.sink.Sink(ctx, m, par, false)
			if err != nil {
				return fmt.Errorf("sink failed: %v\n", err)
			}
		}
		return nil
	})
}

func (h *wordcountSplitFlatMap) wordcount_split(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp, false)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src, sink, msgSerde, err := getSrcSink(ctx, sp, input_stream, output_stream)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	var matchStr = regexp.MustCompile(`\w+`)
	splitter := processor.FlatMapperFunc(func(m commtypes.Message) ([]commtypes.Message, error) {
		val := m.Value.(string)
		val = strings.ToLower(val)
		var splitMsgs []commtypes.Message
		splits := matchStr.FindAllString(val, -1)
		for _, s := range splits {
			if s != "" {
				splitMsgs = append(splitMsgs, commtypes.Message{Key: s, Value: s, Timestamp: m.Timestamp})
			}
		}
		return splitMsgs, nil
	})

	funcName := "wcsplitter"
	procArgs := &wordcountSplitterProcessArg{
		src:              src,
		sink:             sink,
		output_stream:    output_stream,
		splitter:         splitter,
		splitLatencies:   make([]int, 0),
		parNum:           sp.ParNum,
		numOutPartition:  sp.NumOutPartition,
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
		// fmt.Fprintf(os.Stderr, "word count counter function enables exactly once semantics\n")
		srcs := make(map[string]processor.Source)
		srcs[sp.InputTopicNames[0]] = src
		streamTaskArgs := transaction.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			MsgSerde:        msgSerde,
			Srcs:            srcs,
			OutputStream:    output_stream,
			QueryInput:      sp,
			TransactionalId: fmt.Sprintf("%s-%s-%d", funcName, sp.InputTopicNames[0], sp.ParNum),
			FixedOutParNum:  0,
		}
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, &streamTaskArgs,
			func(procArgs interface{}, trackParFunc transaction.TrackKeySubStreamFunc, recordFinishFunc transaction.RecordPrevInstanceFinishFunc) {
				procArgs.(*wordcountSplitterProcessArg).trackParFunc = trackParFunc
				procArgs.(*wordcountSplitterProcessArg).recordFinishFunc = recordFinishFunc
			}, &task)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["split"] = procArgs.splitLatencies
		}
		return ret
	}
	streamTaskArgs := transaction.StreamTaskArgs{
		ProcArgs: procArgs,
		Duration: time.Duration(sp.Duration) * time.Second,
	}
	ret := task.Process(ctx, &streamTaskArgs)
	if ret != nil && ret.Success {
		ret.Latencies["src"] = src.GetLatency()
		ret.Latencies["sink"] = sink.GetLatency()
		ret.Latencies["split"] = procArgs.splitLatencies
	}
	return ret
}
