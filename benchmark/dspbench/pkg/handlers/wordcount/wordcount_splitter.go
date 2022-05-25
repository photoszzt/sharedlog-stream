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
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
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
) (*source_sink.MeteredSource,
	*source_sink.MeteredSyncSink,
	error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	inConfig := &source_sink.StreamSourceConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: commtypes.StringSerde{},
			MsgSerde: msgSerde,
		},
	}
	outConfig := &source_sink.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: commtypes.StringSerde{},
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := source_sink.NewMeteredSource(source_sink.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := source_sink.NewMeteredSyncSink(source_sink.NewShardedSharedLogStreamSyncSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

type wordcountSplitterProcessArg struct {
	src             *source_sink.MeteredSource
	output_stream   *sharedlog_stream.ShardedSharedLogStream
	splitter        processor.FlatMapperFunc
	trackParFunc    tran_interface.TrackKeySubStreamFunc
	splitLatencies  []int
	numOutPartition uint8
	proc_interface.BaseProcArgsWithSink
}

func (a *wordcountSplitterProcessArg) Source() source_sink.Source { return a.src }

func (h *wordcountSplitFlatMap) process(ctx context.Context,
	t *transaction.StreamTask,
	argsTmp interface{},
) *common.FnOutput {
	args := argsTmp.(*wordcountSplitterProcessArg)
	return execution.CommonProcess(ctx, t, args, func(t *transaction.StreamTask, msg commtypes.MsgAndSeq) error {
		t.CurrentOffset[args.src.TopicName()] = msg.LogSeqNum
		splitStart := time.Now()
		msgs, err := args.splitter(msg.Msg)
		if err != nil {
			return fmt.Errorf("splitter failed: %v", err)
		}
		splitLat := time.Since(splitStart)
		args.splitLatencies = append(args.splitLatencies, int(splitLat.Microseconds()))
		for _, m := range msgs {
			hashed := hashKey(m.Key.(string))
			par := uint8(hashed % uint32(args.numOutPartition))
			err = args.trackParFunc(ctx, m.Key, args.Sinks()[0].KeySerde(), args.Sinks()[0].TopicName(), par)
			if err != nil {
				return fmt.Errorf("add topic partition failed: %v", err)
			}
			err = args.Sinks()[0].Produce(ctx, m, par, false)
			if err != nil {
				return fmt.Errorf("sink failed: %v", err)
			}
		}
		return nil
	})
}

func (h *wordcountSplitFlatMap) wordcount_split(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_streams, err := benchutil.GetShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	debug.Assert(len(output_streams) == 1, "expected only one output stream")
	src, sink, err := getSrcSink(ctx, sp, input_stream, output_streams[0])
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	src.SetInitialSource(true)

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
		src:                  src,
		output_stream:        output_streams[0],
		splitter:             splitter,
		splitLatencies:       make([]int, 0),
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink([]source_sink.Sink{sink}, funcName, sp.ScaleEpoch, sp.ParNum),
		numOutPartition:      sp.NumOutPartitions[0],
		trackParFunc:         tran_interface.DefaultTrackSubstreamFunc,
	}

	task := transaction.StreamTask{
		ProcessFunc:   h.process,
		CurrentOffset: make(map[string]uint64),
	}

	srcs := []source_sink.Source{src}
	sinks := []source_sink.Sink{sink}
	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["split"] = procArgs.splitLatencies
		ret.Counts["sink"] = sink.GetCount()
		ret.Counts["src"] = src.GetCount()
	}
	if sp.EnableTransaction {
		// fmt.Fprintf(os.Stderr, "word count counter function enables exactly once semantics\n")
		transactionalID := fmt.Sprintf("%s-%s-%d", funcName, sp.InputTopicNames[0], sp.ParNum)
		streamTaskArgs := transaction.NewStreamTaskArgsTransaction(h.env, transactionalID, procArgs, srcs, sinks)
		benchutil.UpdateStreamTaskArgsTransaction(sp, streamTaskArgs)
		ret := transaction.SetupManagersAndProcessTransactional(ctx, h.env, streamTaskArgs,
			func(procArgs interface{}, trackParFunc tran_interface.TrackKeySubStreamFunc, recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
				procArgs.(*wordcountSplitterProcessArg).trackParFunc = trackParFunc
				procArgs.(*wordcountSplitterProcessArg).SetRecordFinishFunc(recordFinishFunc)
			}, &task)
		if ret != nil && ret.Success {
			update_stats(ret)
		}
		return ret
	}
	streamTaskArgs := transaction.NewStreamTaskArgs(h.env, procArgs, srcs, sinks)
	benchutil.UpdateStreamTaskArgs(sp, streamTaskArgs)
	ret := task.Process(ctx, streamTaskArgs)
	if ret != nil && ret.Success {
		update_stats(ret)
	}
	return ret
}
