package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	datatype "sharedlog-stream/benchmark/lat_tp/pkg/data_type"
	nexmarkutils "sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/utils"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type sharedlogProduceBenchHandler struct {
	env     types.Environment
	bufPush bool
}

func NewSharedlogProduceBenchHandler(env types.Environment) types.FuncHandler {
	return &sharedlogProduceBenchHandler{
		env:     env,
		bufPush: utils.CheckBufPush(),
	}
}

func (h *sharedlogProduceBenchHandler) Call(ctx context.Context, input []byte) ([]byte, error) {
	parsedInput := &common.BenchSourceParam{}
	err := json.Unmarshal(input, parsedInput)
	if err != nil {
		return nil, err
	}
	output := h.sharedlogProduceBench(ctx, parsedInput)
	encodedOutput, err := json.Marshal(output)
	if err != nil {
		panic(err)
	}
	return nexmarkutils.CompressData(encodedOutput), nil
}

func (h *sharedlogProduceBenchHandler) sharedlogProduceBench(ctx context.Context, sp *common.BenchSourceParam) *common.FnOutput {
	content, err := os.ReadFile(sp.FileName)
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	// latencies := make([]int, 0, 128)
	numEvents := sp.NumEvents
	duration := time.Duration(sp.Duration) * time.Second
	nEmitEvent := uint32(0)
	stream, err := sharedlog_stream.NewShardedSharedLogStream(h.env, sp.TopicName, sp.NumOutPartition, commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	var ptSerde datatype.PayloadTsMsgpSerde
	timeGapUs := time.Duration(1000000/sp.Tps) * time.Microsecond
	msgChan := make(chan sharedlog_stream.PayloadToPush, 100000)
	msgErrChan := make(chan error)
	var wg sync.WaitGroup
	streamPusher := sharedlog_stream.StreamPush{
		MsgChan:    msgChan,
		MsgErrChan: msgErrChan,
		Stream:     stream,
		BufPush:    h.bufPush,
	}
	wg.Add(1)
	go streamPusher.AsyncStreamPush(ctx, &wg, commtypes.EmptyProducerId)
	streamPusher.InitFlushTimer(time.Duration(sp.FlushMs) * time.Millisecond)
	startTime := time.Now()
	next := time.Now()

	for {
		select {
		case merr := <-msgErrChan:
			return &common.FnOutput{Success: false, Message: merr.Error()}
		default:
		}
		if (duration != 0 && time.Since(startTime) >= duration) || (numEvents != 0 && nEmitEvent == numEvents) {
			break
		}
		// procStart := time.Now()
		parNum := nEmitEvent % uint32(sp.NumOutPartition)
		next = next.Add(timeGapUs)
		pt := datatype.PayloadTs{
			Payload: content,
			Ts:      next.UnixMicro(),
		}
		encoded, err := ptSerde.Encode(&pt)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		now := time.Now()
		if next.After(now) {
			time.Sleep(next.Sub(now))
		}
		/*
			_, err = stream.Push(ctx, encoded, uint8(parNum), false, false)
			if err != nil {
				return &common.FnOutput{Success: false, Message: err.Error()}
			}
		*/
		streamPusher.MsgChan <- sharedlog_stream.PayloadToPush{Payload: encoded, Partitions: []uint8{uint8(parNum)}, IsControl: false}
		// elapsed := time.Since(procStart)
		// latencies = append(latencies, int(elapsed.Microseconds()))
		nEmitEvent += 1
	}
	close(msgChan)
	wg.Wait()
	if h.bufPush {
		err = stream.Flush(ctx, commtypes.EmptyProducerId)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[Error] Flush failed: %v\n", err)
		}
	}
	return &common.FnOutput{
		Success:  true,
		Duration: time.Since(startTime).Seconds(),
		Counts:   map[string]uint64{"prod": uint64(nEmitEvent)},
		// Latencies: map[string][]int{"e2e": latencies},
	}
}
