package wordcount

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"regexp"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/common/benchutil"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"strings"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
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

func getSrcSink(ctx context.Context, sp *common.QueryInput, input_stream *sharedlog_stream.ShardedSharedLogStream, output_stream *sharedlog_stream.ShardedSharedLogStream) (*processor.MeteredSource, *processor.MeteredSink, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: commtypes.StringDecoder{},
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder:   commtypes.StringEncoder{},
		ValueEncoder: commtypes.StringEncoder{},
		MsgEncoder:   msgSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, nil
}

type wordcountSplitterProcessArg struct {
	src             *processor.MeteredSource
	sink            *processor.MeteredSink
	output_stream   *sharedlog_stream.ShardedSharedLogStream
	splitter        processor.FlatMapperFunc
	splitLatencies  []int
	parNum          uint8
	numOutPartition uint8
}

func (h *wordcountSplitFlatMap) process(ctx context.Context,
	argsTmp interface{},
	trackParFunc func([]uint8) error,
) (uint64, *common.FnOutput) {
	args := argsTmp.(*wordcountSplitterProcessArg)
	currentOffset := uint64(0)
	gotMsgs, err := args.src.Consume(ctx, args.parNum)
	if err != nil {
		if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
			return currentOffset, &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
		}
		return currentOffset, &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("consumed failed: %v\n", err),
		}
	}
	for _, msg := range gotMsgs {
		if msg.Msg.Value == nil {
			continue
		}
		currentOffset = msg.LogSeqNum
		splitStart := time.Now()
		msgs, err := args.splitter(msg.Msg)
		if err != nil {
			return currentOffset, &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("splitter failed: %v\n", err),
			}
		}
		splitLat := time.Since(splitStart)
		args.splitLatencies = append(args.splitLatencies, int(splitLat.Microseconds()))
		for _, m := range msgs {
			h := hashKey(m.Key.(string))
			par := uint8(h % uint32(args.numOutPartition))
			err = trackParFunc([]uint8{par})
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("add topic partition failed: %v\n", err),
				}
			}
			err = args.sink.Sink(ctx, m, par, false)
			if err != nil {
				return currentOffset, &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink failed: %v\n", err),
				}
			}
		}
	}
	return currentOffset, nil
}

func (h *wordcountSplitFlatMap) wordcount_split(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src, sink, err := getSrcSink(ctx, sp, input_stream, output_stream)
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

	procArgs := &wordcountSplitterProcessArg{
		src:             src,
		sink:            sink,
		output_stream:   output_stream,
		splitter:        splitter,
		splitLatencies:  make([]int, 0),
		parNum:          sp.ParNum,
		numOutPartition: sp.NumOutPartition,
	}

	task := sharedlog_stream.StreamTask{
		ProcessFunc: h.process,
	}

	if sp.EnableTransaction {
		fmt.Fprintf(os.Stderr, "word count counter function enables exactly once semantics\n")
		streamTaskArgs := &sharedlog_stream.StreamTaskArgsTransaction{
			ProcArgs:        procArgs,
			Env:             h.env,
			Src:             src,
			OutputStream:    output_stream,
			QueryInput:      sp,
			TransactionalId: fmt.Sprintf("wordcount-splitter-%s-%d", sp.InputTopicName, sp.ParNum),
			FixedOutParNum:  0,
			TestParams:      sp.TestParams,
		}
		ret := task.ProcessWithTransaction(ctx, streamTaskArgs)
		if ret != nil && ret.Success {
			ret.Latencies["src"] = src.GetLatency()
			ret.Latencies["sink"] = sink.GetLatency()
			ret.Latencies["split"] = procArgs.splitLatencies
		}
		return ret
	}
	streamTaskArgs := sharedlog_stream.StreamTaskArgs{
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
