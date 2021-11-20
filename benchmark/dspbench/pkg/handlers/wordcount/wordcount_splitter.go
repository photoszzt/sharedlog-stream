package wordcount

import (
	"context"
	"encoding/json"
	"errors"
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
	"sync/atomic"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/sync/errgroup"
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
		Timeout:      time.Duration(sp.Duration) * time.Second,
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

func (h *wordcountSplitFlatMap) processWithTransaction(
	ctx context.Context, sp *common.QueryInput,
	src *processor.MeteredSource, sink *processor.MeteredSink,
	output_stream *sharedlog_stream.ShardedSharedLogStream, splitter processor.FlatMapperFunc,
) *common.FnOutput {
	splitLatencies := make([]int, 0, 128)
	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second

	transactionalId := fmt.Sprintf("wordcount-splitter-%s-%d", sp.InputTopicName, sp.ParNum)
	tm, appId, appEpoch, err := benchutil.SetupTransactionManager(ctx, h.env, transactionalId, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	tm.RecordTopicStreams(sp.OutputTopicName, output_stream)
	hasLiveTransaction := false
	trackConsumePar := false
	currentOffset := uint64(0)
	commitTimer := time.Now()
	commitEvery := time.Duration(sp.CommitEvery) * time.Millisecond

	shouldStop := uint32(0)
	errBackground, mctx := errgroup.WithContext(ctx)
	errBackground.Go(func() error {
		return tm.MonitorTransactionLog(mctx, &shouldStop)
	})

	startTime := time.Now()
	for atomic.LoadUint32(&shouldStop) == 0 {
		timeSinceTranStart := time.Since(commitTimer)
		timeout := duration != 0 && time.Since(startTime) >= duration
		if (commitEvery != 0 && timeSinceTranStart > commitEvery) || timeout {
			err = tm.AppendOffset(ctx, sharedlog_stream.OffsetConfig{
				TopicToTrack: sp.InputTopicName,
				AppId:        appId,
				AppEpoch:     appEpoch,
				Partition:    sp.ParNum,
				Offset:       currentOffset,
			})
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
				}
			}
			err = tm.CommitTransaction(ctx)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("commit failed: %v\n", err),
				}
			}
			hasLiveTransaction = false
			trackConsumePar = false
		}
		if timeout {
			err = tm.Close()
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("close transaction manager: %v\n", err),
				}
			}
			break
		}
		if !hasLiveTransaction {
			err = tm.BeginTransaction(ctx)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("transaction begin failed: %v\n", err),
				}
			}
			hasLiveTransaction = true
			commitTimer = time.Now()
		}

		procStart := time.Now()
		gotMsgs, err := src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
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
		if !trackConsumePar {
			err = tm.AddOffsets(ctx, sp.InputTopicName, []uint8{sp.ParNum})
			if err != nil {
				return &common.FnOutput{
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
			msgs, err := splitter(msg.Msg)
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
				err = tm.AddTopicPartition(ctx, output_stream.TopicName(), []uint8{par})
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add topic partition failed: %v\n", err),
					}
				}
				err = sink.Sink(ctx, m, par, false)
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
	err = errBackground.Wait()
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":   latencies,
			"src":   src.GetLatency(),
			"sink":  sink.GetLatency(),
			"split": splitLatencies,
		},
	}
}

func (h *wordcountSplitFlatMap) process(
	ctx context.Context, sp *common.QueryInput,
	src *processor.MeteredSource, sink *processor.MeteredSink,
	output_stream *sharedlog_stream.ShardedSharedLogStream, splitter processor.FlatMapperFunc,
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
		gotMsgs, err := src.Consume(ctx, sp.ParNum)
		if err != nil {
			if errors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
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
			msgs, err := splitter(msg.Msg)
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
				err = sink.Sink(ctx, m, par, false)
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
			"src":   src.GetLatency(),
			"sink":  sink.GetLatency(),
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

	if sp.EnableTransaction {
		fmt.Fprintf(os.Stderr, "word count counter function enables exactly once semantics\n")
		return h.processWithTransaction(ctx, sp, src, sink, output_stream, splitter)
	}
	return h.process(ctx, sp, src, sink, output_stream, splitter)
}
