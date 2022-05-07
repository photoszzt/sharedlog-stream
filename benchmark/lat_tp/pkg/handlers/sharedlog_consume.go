package handlers

import (
	"context"
	"encoding/json"
	"sharedlog-stream/benchmark/common"
	datatype "sharedlog-stream/benchmark/lat_tp/pkg/data_type"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type sharedlogConsumeBenchHandler struct {
	env types.Environment
}

func NewSharedlogConsumeBenchHandler(env types.Environment) types.FuncHandler {
	return &sharedlogConsumeBenchHandler{
		env: env,
	}
}

func (h *sharedlogConsumeBenchHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.BenchSourceParam{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.sharedlogConsumeBench(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return utils.CompressData(encodedOutput), nil
}

func (h *sharedlogConsumeBenchHandler) sharedlogConsumeBench(ctx context.Context, sp *common.BenchSourceParam) *common.FnOutput {
	stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, sp.TopicName, sp.NumOutPartition, commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	cm, err := transaction.NewConsumeSeqManager(commtypes.MSGP)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	err = cm.CreateOffsetTopic(h.env, sp.TopicName, sp.NumOutPartition, commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	cm.AddTopicTrackConsumedSeqs(ctx, sp.TopicName, []uint8{0})

	// warmup
	rest := sp.NumEvents
	warmup_dur := time.Duration(0)
	hasUncommitted := false
	if sp.WarmUpEvents > 0 && sp.WarmUpTime > 0 {
		warm_start := time.Now()
		idx_consumed, err := h.runLoop(ctx, stream,
			time.Duration(sp.WarmUpTime)*time.Second, int(sp.WarmUpEvents), cm)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		rest = sp.NumEvents - uint32(idx_consumed)
		warmup_dur = time.Since(warm_start)
	}
	idx := uint32(0)
	var paSerde commtypes.PayloadArrMsgpSerde
	var ptSerde datatype.PayloadTsMsgpSerde
	prod_consume_lat := make([]int, 0, 128)
	duration := time.Duration(sp.Duration)*time.Second - warmup_dur
	off := uint64(0)
	commitTimer := time.NewTicker(common.CommitDuration)
	startTime := time.Now()
	for {
		select {
		case <-commitTimer.C:
			err = commitConsumeSeq(ctx, cm, sp.TopicName, off)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
			hasUncommitted = false
		default:
		}
		if (duration != 0 && time.Since(startTime) >= duration) || (rest > 0 && idx >= rest) {
			break
		}
		_, rawMsgs, err := stream.ReadNext(ctx, 0)
		if err != nil {
			if errors.IsStreamEmptyError(err) {
				// debug.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(100) * time.Microsecond)
				continue
			} else if errors.IsStreamTimeoutError(err) {
				// debug.Fprintf(os.Stderr, "stream time out\n")
				continue
			} else {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
		for _, rawMsg := range rawMsgs {
			idx += 1
			if !rawMsg.IsControl && len(rawMsg.Payload) == 0 {
				continue
			}
			off = rawMsg.LogSeqNum
			if !hasUncommitted {
				hasUncommitted = true
			}
			if rawMsg.IsPayloadArr {
				payloadArrTmp, err := paSerde.Decode(rawMsg.Payload)
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				payloadArr := payloadArrTmp.(commtypes.PayloadArr)
				for _, pBytes := range payloadArr.Payloads {
					ptTmp, err := ptSerde.Decode(pBytes)
					if err != nil {
						return &common.FnOutput{Success: false, Message: err.Error()}
					}
					pt := ptTmp.(datatype.PayloadTs)
					now := time.Now().UnixMicro()
					lat := int(now - pt.Ts)
					debug.Assert(lat > 0, "latency should not be negative")
					prod_consume_lat = append(prod_consume_lat, lat)
				}
			} else {
				ptTmp, err := ptSerde.Decode(rawMsg.Payload)
				if err != nil {
					return &common.FnOutput{Success: false, Message: err.Error()}
				}
				pt := ptTmp.(datatype.PayloadTs)
				now := time.Now().UnixMicro()
				lat := int(now - pt.Ts)
				debug.Assert(lat > 0, "latency should not be negative")
				prod_consume_lat = append(prod_consume_lat, lat)
			}
		}
	}
	if hasUncommitted {
		err := commitConsumeSeq(ctx, cm, stream.TopicName(), off)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return &common.FnOutput{
		Success:   true,
		Duration:  time.Since(startTime).Seconds(),
		Latencies: map[string][]int{"e2e": prod_consume_lat},
	}
}

func commitConsumeSeq(ctx context.Context,
	cm *transaction.ConsumeSeqManager, topicName string, off uint64) error {
	consumedSeqNumConfig := []transaction.ConsumedSeqNumConfig{
		{
			TopicToTrack:   topicName,
			Partition:      0,
			ConsumedSeqNum: off,
		},
	}
	err := cm.AppendConsumedSeqNum(ctx, consumedSeqNumConfig)
	if err != nil {
		return err
	}
	err = cm.Commit(ctx)
	if err != nil {
		return err
	}
	return nil
}

func (h *sharedlogConsumeBenchHandler) runLoop(ctx context.Context,
	stream *sharedlog_stream.ShardedSharedLogStream, duration time.Duration,
	numEvents int, cm *transaction.ConsumeSeqManager,
) (int, error) {
	idx := 0
	var ptSerde datatype.PayloadTsMsgpSerde
	var paSerde commtypes.PayloadArrMsgpSerde
	off := uint64(0)
	hasUncommitted := false
	commitTimer := time.NewTicker(common.CommitDuration)
	startTime := time.Now()
	for {
		select {
		case <-commitTimer.C:
			err := commitConsumeSeq(ctx, cm, stream.TopicName(), off)
			if err != nil {
				return 0, err
			}
			hasUncommitted = false
		default:
		}
		if (duration != 0 && time.Since(startTime) >= duration) || (numEvents != 0 && idx >= numEvents) {
			commitTimer.Stop()
			break
		}
		_, rawMsgs, err := stream.ReadNext(ctx, 0)
		if err != nil {
			if errors.IsStreamEmptyError(err) {
				// debug.Fprintf(os.Stderr, "stream is empty\n")
				time.Sleep(time.Duration(5) * time.Millisecond)
				continue
			} else if errors.IsStreamTimeoutError(err) {
				// debug.Fprintf(os.Stderr, "stream time out\n")
				continue
			} else {
				return 0, err
			}
		}
		for _, rawMsg := range rawMsgs {
			idx += 1
			off = rawMsg.LogSeqNum
			if !hasUncommitted {
				hasUncommitted = true
			}
			if !rawMsg.IsControl && len(rawMsg.Payload) == 0 {
				continue
			}
			if rawMsg.IsPayloadArr {
				payloadArrTmp, err := paSerde.Decode(rawMsg.Payload)
				if err != nil {
					return 0, err
				}
				payloadArr := payloadArrTmp.(commtypes.PayloadArr)
				for _, pBytes := range payloadArr.Payloads {
					_ = pBytes
				}
			} else {
				ptTmp, err := ptSerde.Decode(rawMsg.Payload)
				if err != nil {
					return 0, err
				}
				_ = ptTmp
			}
		}
	}
	if hasUncommitted {
		err := commitConsumeSeq(ctx, cm, stream.TopicName(), off)
		if err != nil {
			return 0, err
		}
		hasUncommitted = false
	}
	return idx, nil
}
