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
	src           *processor.MeteredSource
	sink          *processor.MeteredSink
	output_stream *sharedlog_stream.ShardedSharedLogStream
	splitter      processor.FlatMapperFunc
}

func (h *wordcountSplitFlatMap) processWithTranLoop(
	ctx context.Context, sp *common.QueryInput, args wordcountSplitterProcessArg,
	tm *sharedlog_stream.TransactionManager, appId uint64, appEpoch uint16,
	retc chan *common.FnOutput,
) {
	latencies := make([]int, 0, 128)
	splitLatencies := make([]int, 0, 128)
	hasLiveTransaction := false
	trackConsumePar := false
	currentOffset := uint64(0)
	commitTimer := time.Now()
	commitEvery := time.Duration(sp.CommitEvery) * time.Millisecond
	duration := time.Duration(sp.Duration) * time.Second

	startTime := time.Now()
L:
	for {
		select {
		case <-ctx.Done():
			break L
		default:
		}
		timeSinceTranStart := time.Since(commitTimer)
		timeout := duration != 0 && time.Since(startTime) >= duration
		if (commitEvery != 0 && timeSinceTranStart > commitEvery) || timeout {
			benchutil.TrackOffsetAndCommit(ctx, sharedlog_stream.OffsetConfig{
				TopicToTrack: sp.InputTopicName,
				AppId:        appId,
				AppEpoch:     appEpoch,
				Partition:    sp.ParNum,
				Offset:       currentOffset,
			}, tm, &hasLiveTransaction, &trackConsumePar, retc)
		}
		if timeout {
			err := tm.Close()
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("close transaction manager: %v\n", err),
				}
			}
			break
		}
		if !hasLiveTransaction {
			err := tm.BeginTransaction(ctx)
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("transaction begin failed: %v\n", err),
				}
			}
			hasLiveTransaction = true
			commitTimer = time.Now()
		}

		procStart := time.Now()
		gotMsgs, err := args.src.Consume(ctx, sp.ParNum)
		if err != nil {
			if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				retc <- &common.FnOutput{
					Success:   true,
					Message:   err.Error(),
					Latencies: map[string][]int{"e2e": latencies},
					Duration:  time.Since(startTime).Seconds(),
				}
			}
			retc <- &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("consumed failed: %v\n", err),
			}
		}
		if !trackConsumePar {
			err = tm.AddOffsets(ctx, sp.InputTopicName, []uint8{sp.ParNum})
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("add offsets failed: %v\n", err),
				}
			}
			trackConsumePar = true
		}
		for _, msg := range gotMsgs {
			if msg.Msg.Value == nil {
				continue
			}
			splitStart := time.Now()
			msgs, err := args.splitter(msg.Msg)
			if err != nil {
				retc <- &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("splitter failed: %v\n", err),
				}
			}
			splitLat := time.Since(splitStart)
			splitLatencies = append(splitLatencies, int(splitLat.Microseconds()))
			for _, m := range msgs {
				h := hashKey(m.Key.(string))
				par := uint8(h % uint32(sp.NumOutPartition))
				err = tm.AddTopicPartition(ctx, args.output_stream.TopicName(), []uint8{par})
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add topic partition failed: %v\n", err),
					}
				}
				err = args.sink.Sink(ctx, m, par, false)
				if err != nil {
					retc <- &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("sink failed: %v\n", err),
					}
				}
			}
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	retc <- &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":   latencies,
			"src":   args.src.GetLatency(),
			"sink":  args.sink.GetLatency(),
			"split": splitLatencies,
		},
	}
}

func (h *wordcountSplitFlatMap) processWithTransaction(
	ctx context.Context, sp *common.QueryInput, args wordcountSplitterProcessArg,
) *common.FnOutput {

	transactionalId := fmt.Sprintf("wordcount-splitter-%s-%d", sp.InputTopicName, sp.ParNum)
	tm, appId, appEpoch, err := benchutil.SetupTransactionManager(ctx, h.env, transactionalId, sp, args.src)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	tm.RecordTopicStreams(sp.OutputTopicName, args.output_stream)

	monitorQuit := make(chan struct{})
	monitorErrc := make(chan error)

	dctx, dcancel := context.WithCancel(ctx)
	go tm.MonitorTransactionLog(ctx, monitorQuit, monitorErrc, dcancel)

	retc := make(chan *common.FnOutput)
	go h.processWithTranLoop(dctx, sp, args, tm, appId, appEpoch, retc)
	for {
		select {
		case ret := <-retc:
			close(monitorQuit)
			return ret
		case merr := <-monitorErrc:
			close(monitorQuit)
			if merr != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("monitor failed: %v", merr),
				}
			}
		}
	}
}

func (h *wordcountSplitFlatMap) process(
	ctx context.Context, sp *common.QueryInput, args wordcountSplitterProcessArg,
) *common.FnOutput {
	splitLatencies := make([]int, 0, 128)
	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second

	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		gotMsgs, err := args.src.Consume(ctx, sp.ParNum)
		if err != nil {
			if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
				return &common.FnOutput{
					Success:   true,
					Message:   err.Error(),
					Latencies: map[string][]int{"e2e": latencies},
					Duration:  time.Since(startTime).Seconds(),
				}
			}
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("consumed failed: %v\n", err),
			}
		}
		for _, msg := range gotMsgs {
			if msg.Msg.Value == nil {
				continue
			}
			splitStart := time.Now()
			msgs, err := args.splitter(msg.Msg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("splitter failed: %v\n", err),
				}
			}
			splitLat := time.Since(splitStart)
			splitLatencies = append(splitLatencies, int(splitLat.Microseconds()))
			for _, m := range msgs {
				h := hashKey(m.Key.(string))
				par := uint8(h % uint32(sp.NumOutPartition))
				fmt.Fprintf(os.Stderr, "append %v to %d\n", m, par)
				err = args.sink.Sink(ctx, m, par, false)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("sink failed: %v\n", err),
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
			"e2e":   latencies,
			"src":   args.src.GetLatency(),
			"sink":  args.sink.GetLatency(),
			"split": splitLatencies,
		},
	}
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

	args := wordcountSplitterProcessArg{
		src:           src,
		sink:          sink,
		output_stream: output_stream,
		splitter:      splitter,
	}

	if sp.EnableTransaction {
		fmt.Fprintf(os.Stderr, "word count counter function enables exactly once semantics\n")
		return h.processWithTransaction(ctx, sp, args)
	}
	return h.process(ctx, sp, args)
}
