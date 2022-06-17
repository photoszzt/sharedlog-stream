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
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/execution"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream_task"
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
) (*producer_consumer.MeteredConsumer, *producer_consumer.MeteredProducer, error) {
	serdeFormat := commtypes.SerdeFormat(sp.SerdeFormat)
	msgSerde, err := commtypes.GetMsgSerde(serdeFormat)
	if err != nil {
		return nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	inConfig := &producer_consumer.StreamConsumerConfig{
		Timeout: common.SrcConsumeTimeout,
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: commtypes.StringSerde{},
			MsgSerde: msgSerde,
		},
	}
	outConfig := &producer_consumer.StreamSinkConfig{
		KVMsgSerdes: commtypes.KVMsgSerdes{
			KeySerde: commtypes.StringSerde{},
			ValSerde: commtypes.StringSerde{},
			MsgSerde: msgSerde,
		},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := producer_consumer.NewMeteredConsumer(producer_consumer.NewShardedSharedLogStreamConsumer(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := producer_consumer.NewMeteredProducer(producer_consumer.NewShardedSharedLogStreamProducer(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, nil
}

type wordcountSplitterProcessArg struct {
	splitter       processor.FlatMapperFunc
	splitLatencies []int
	proc_interface.BaseExecutionContext
	numOutPartition uint8
}

func (h *wordcountSplitFlatMap) procMsg(ctx context.Context, msg commtypes.Message, argsTmp interface{}) error {
	args := argsTmp.(*wordcountSplitterProcessArg)
	splitStart := time.Now()
	msgs, err := args.splitter(msg)
	if err != nil {
		return fmt.Errorf("splitter failed: %v", err)
	}
	splitLat := time.Since(splitStart)
	args.splitLatencies = append(args.splitLatencies, int(splitLat.Microseconds()))
	for _, m := range msgs {
		hashed := hashKey(m.Key.(string))
		par := uint8(hashed % uint32(args.numOutPartition))
		err = args.TrackParFunc()(ctx, m.Key, args.Producers()[0].KeySerde(),
			args.Producers()[0].TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = args.Producers()[0].Produce(ctx, m, par, false)
		if err != nil {
			return fmt.Errorf("sink failed: %v", err)
		}
	}
	return nil
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
	srcs := []producer_consumer.MeteredConsumerIntr{src}
	sinks := []producer_consumer.MeteredProducerIntr{sink}
	procArgs := &wordcountSplitterProcessArg{
		splitter:       splitter,
		splitLatencies: make([]int, 0),
		BaseExecutionContext: proc_interface.NewExecutionContext(srcs, sinks,
			funcName, sp.ScaleEpoch, sp.ParNum),
		numOutPartition: sp.NumOutPartitions[0],
	}

	task := stream_task.NewStreamTaskBuilder().
		AppProcessFunc(func(ctx context.Context, task *stream_task.StreamTask, argsTmp interface{}) *common.FnOutput {
			args := argsTmp.(proc_interface.ExecutionContext)
			return execution.CommonProcess(ctx, task, args, h.procMsg)
		}).
		InitFunc(func(progArgs interface{}) {
			src.StartWarmup()
			sink.StartWarmup()
		}).Build()

	update_stats := func(ret *common.FnOutput) {
		ret.Latencies["split"] = procArgs.splitLatencies
		ret.Counts["sink"] = sink.GetCount()
		ret.Counts["src"] = src.GetCount()
	}
	transactionalID := fmt.Sprintf("%s-%s-%d", funcName, sp.InputTopicNames[0], sp.ParNum)
	streamTaskArgs := benchutil.UpdateStreamTaskArgs(sp,
		stream_task.NewStreamTaskArgsBuilder(h.env, procArgs, transactionalID)).Build()
	return task.ExecuteApp(ctx, streamTaskArgs, update_stats)
}
