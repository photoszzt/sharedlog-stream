package wordcount

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
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

func (h *wordcountSplitFlatMap) process(ctx context.Context, sp *common.QueryInput) *common.FnOutput {
	input_stream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, sp.InputTopicName, uint32(sp.NumInPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
		}
	}
	output_stream, err := sharedlog_stream.NewShardedSharedLogStream(ctx, h.env, sp.OutputTopicName, uint32(sp.NumOutPartition))
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("NewShardedSharedLogStream failed: %v", err),
		}
	}

	var weSerde processor.Serde
	var seSerde processor.Serde
	if sp.SerdeFormat == uint8(processor.JSON) {
		weSerde = WordEventJSONSerde{}
		seSerde = SentenceEventJSONSerde{}
	} else if sp.SerdeFormat == uint8(processor.MSGP) {
		weSerde = WordEventMsgpSerde{}
		seSerde = SentenceEventMsgpSerde{}
	} else {
		return &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("serde format should be either json or msgp; but %v is given", sp.SerdeFormat),
		}
	}
	msgSerde, err := processor.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
	}

	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(sp.Duration) * time.Second,
		KeyDecoder:   processor.StringDecoder{},
		ValueDecoder: seSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeyEncoder:   processor.StringEncoder{},
		ValueEncoder: weSerde,
		MsgEncoder:   msgSerde,
	}
	src := sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig)
	sink := sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig)
	startTime := time.Now()
	for {
		msg, err := src.Consume(uint32(sp.PartNum))
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
		splitter := processor.FlatMapperFunc(func(m processor.Message) ([]processor.Message, error) {
			val := m.Value.(*SentenceEvent)
			var splitMsgs []processor.Message
			splits := strings.Split(val.Sentence, " ")
			for _, s := range splits {
				if s != "" {
					splitMsgs = append(splitMsgs, processor.Message{Key: s, Value: &WordEvent{Word: s, Ts: val.Ts}, Timestamp: val.Ts})
				}
			}
			return splitMsgs, nil
		})
		msgs, err := splitter(msg)
		if err != nil {
			return &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("splitter failed: %v\n", err),
			}
		}

		for _, m := range msgs {
			h := hashKey(m.Key.(string))
			h = h % uint32(sp.NumOutPartition)
			err = sink.Sink(m, uint32(h))
			if err != nil {
				return &common.FnOutput{
					Success: false,
					Message: fmt.Sprintf("sink failed: %v\n", err),
				}
			}
		}
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
	}
}
