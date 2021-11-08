package wordcount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"regexp"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
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
	output := h.process(ctx, sp)
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

func getShardedInputOutputStreams(ctx context.Context, env types.Environment, input *common.QueryInput) (*sharedlog_stream.ShardedSharedLogStream, *sharedlog_stream.ShardedSharedLogStream, error) {
	inputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicName, uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	err = inputStream.InitStream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("InitStream failed: %v", err)
	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	err = outputStream.InitStream(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("InitStream failed: %v", err)
	}
	return inputStream, outputStream, nil
}

func (h *wordcountSplitFlatMap) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := getShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("get msg serde failed: %v", err),
		}
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
	src := sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig)
	sink := sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig)
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
	srcLatencies := make([]int, 0, 128)
	sinkLatencies := make([]int, 0, 128)
	splitLatencies := make([]int, 0, 128)
	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second

	if sp.EnableTransaction {
		transactionalId := "wordcount"
		tm, err := sharedlog_stream.NewTransactionManager(ctx, h.env, transactionalId, commtypes.SerdeFormat(sp.SerdeFormat))
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		appId, appEpoch := tm.InitTransaction()

		err = tm.CreateOffsetTopic(sp.InputTopicName, uint8(sp.NumInPartition))
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("create offset topic failed: %v", err),
			}
		}
		tm.RecordTopicStreams(sp.OutputTopicName, output_stream)
		hasLiveTransaction := false
		startTime := time.Now()
		trackConsumePar := false
		currentOffset := uint64(0)
		commitTimer := time.Now()
		commitEvery := time.Duration(1) * time.Millisecond
		var msg commtypes.Message

		for {
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
				err = tm.CommitTransaction(ctx, appId, appEpoch)
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
				err = tm.BeginTransaction(ctx, appId, appEpoch)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("transaction begin failed: %v", err),
					}
				}
				hasLiveTransaction = true
				commitTimer = time.Now()
			}

			procStart := time.Now()
			msg, currentOffset, err = src.Consume(ctx, sp.ParNum)
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
					Message: fmt.Sprintf("consumed failed: %v", err),
				}
			}
			if !trackConsumePar {
				err = tm.AddOffsets(sp.InputTopicName, sp.ParNum)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add offsets failed: %v", err),
					}
				}
				trackConsumePar = true
			}
			srcLat := time.Since(procStart)
			srcLatencies = append(srcLatencies, int(srcLat.Microseconds()))
			if msg.Value == nil {
				continue
			}
			splitStart := time.Now()
			msgs, err := splitter(msg)
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
				err = tm.AddTopicPartition(output_stream.TopicName(), par)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add topic partition failed: %v\n", err),
					}
				}
				sinkStart := time.Now()
				err = sink.Sink(ctx, m, par)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("sink failed: %v\n", err),
					}
				}
				sinkLat := time.Since(sinkStart)
				sinkLatencies = append(sinkLatencies, int(sinkLat.Microseconds()))
			}
			elapsed := time.Since(procStart)
			latencies = append(latencies, int(elapsed.Microseconds()))
		}
		return &common.FnOutput{
			Success:  true,
			Duration: time.Since(startTime).Seconds(),
			Latencies: map[string][]int{
				"e2e":   latencies,
				"src":   srcLatencies,
				"sink":  sinkLatencies,
				"split": splitLatencies,
			},
		}
	}
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msg, _, err := src.Consume(ctx, sp.ParNum)
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
				Message: fmt.Sprintf("consumed failed: %v", err),
			}
		}
		srcLat := time.Since(procStart)
		srcLatencies = append(srcLatencies, int(srcLat.Microseconds()))
		if msg.Value == nil {
			continue
		}
		splitStart := time.Now()
		msgs, err := splitter(msg)
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
			sinkStart := time.Now()
			err = sink.Sink(ctx, m, par)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink failed: %v\n", err),
				}
			}
			sinkLat := time.Since(sinkStart)
			sinkLatencies = append(sinkLatencies, int(sinkLat.Microseconds()))
		}
		elapsed := time.Since(procStart)
		latencies = append(latencies, int(elapsed.Microseconds()))
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Latencies: map[string][]int{
			"e2e":   latencies,
			"src":   srcLatencies,
			"sink":  sinkLatencies,
			"split": splitLatencies,
		},
	}
}
