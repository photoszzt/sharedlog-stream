package wordcount

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"time"

	"cs.utexas.edu/zjia/faas/types"
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
	output := h.process(ctx, sp)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		return nil, err
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *wordcountCounterAgg) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, output_stream, err := getShardedInputOutputStreams(ctx, h.env, sp)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

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
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(sp.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: commtypes.StringDecoder{},
		MsgDecoder:   msgSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	mp := &store.MaterializeParam{
		KeySerde:   commtypes.StringSerde{},
		ValueSerde: vtSerde,
		MsgSerde:   msgSerde,
		StoreName:  sp.OutputTopicName,
		Changelog:  output_stream,
		ParNum:     sp.ParNum,
	}

	store := store.NewInMemoryKeyValueStoreWithChangelog(mp)
	p := processor.NewMeteredProcessor(processor.NewStreamAggregateProcessor(store,
		processor.InitializerFunc(func() interface{} {
			return uint64(0)
		}),
		processor.AggregatorFunc(func(key interface{}, value interface{}, agg interface{}) interface{} {
			aggVal := agg.(uint64)
			return aggVal + 1
		})))

	latencies := make([]int, 0, 128)
	duration := time.Duration(sp.Duration) * time.Second
	if sp.EnableTransaction {
		transactionalId := fmt.Sprintf("wordcount-counter-%s-%d", sp.InputTopicName, sp.ParNum)
		tm, err := sharedlog_stream.NewTransactionManager(ctx, h.env, transactionalId, commtypes.SerdeFormat(sp.SerdeFormat))
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("NewTransactionManager failed: %v\n", err),
			}
		}
		appId, appEpoch, err := tm.InitTransaction(ctx)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("InitTransaction failed: %v\n", err),
			}
		}

		err = tm.CreateOffsetTopic(sp.InputTopicName, sp.NumInPartition)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("create offset topic failed: %v", err),
			}
		}
		tm.RecordTopicStreams(sp.OutputTopicName, output_stream)
		tm.RecordTopicStreams(mp.Changelog.TopicName(), mp.Changelog)
		hasLiveTransaction := false
		startTime := time.Now()
		trackConsumePar := false
		currentOffset := uint64(0)
		var commitTimer time.Time
		commitEvery := time.Duration(sp.CommitEvery) * time.Millisecond
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
						Message: fmt.Sprintf("append offset failed: %v\n", err),
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
						Message: fmt.Sprintf("transaction begin failed: %v\n", err),
					}
				}
				hasLiveTransaction = true
				commitTimer = time.Now()
			}
			procStart := time.Now()
			msgs, err := src.Consume(ctx, sp.ParNum)
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
					Message: err.Error(),
				}
			}
			if !trackConsumePar {
				err = tm.AddOffsets(ctx, sp.InputTopicName, []uint8{sp.ParNum})
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: fmt.Sprintf("add offsets failed: %v", err),
					}
				}
				trackConsumePar = true
			}
			for _, msg := range msgs {
				_, err = p.ProcessAndReturn(ctx, msg.Msg)
				if err != nil {
					return &common.FnOutput{
						Success: false,
						Message: err.Error(),
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
				"count": p.GetLatency(),
			},
		}

	}
	startTime := time.Now()
	for {
		if duration != 0 && time.Since(startTime) >= duration {
			break
		}
		procStart := time.Now()
		msgs, err := src.Consume(ctx, sp.ParNum)
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
				Message: err.Error(),
			}
		}
		for _, msg := range msgs {
			_, err = p.ProcessAndReturn(ctx, msg.Msg)
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: err.Error(),
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
			"count": p.GetLatency(),
		},
	}
}
