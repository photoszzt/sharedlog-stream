package handlers

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sync"
	"time"

	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"

	"golang.org/x/xerrors"
)

func only_bid(msg *commtypes.Message) (bool, error) {
	event := msg.Value.(*ntypes.Event)
	return event.Etype == ntypes.BID, nil
}

func getEventSerde(serdeFormat uint8) (commtypes.Serde, error) {
	if serdeFormat == uint8(commtypes.JSON) {
		return ntypes.EventJSONSerde{}, nil
	} else if serdeFormat == uint8(commtypes.MSGP) {
		return ntypes.EventMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}

func getPersonTimeSerde(serdeFormat uint8) (commtypes.Serde, error) {
	if serdeFormat == uint8(commtypes.JSON) {
		return ntypes.PersonTimeJSONSerde{}, nil
	} else if serdeFormat == uint8(commtypes.MSGP) {
		return ntypes.PersonTimeMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}

type JoinWorkerFunc func(c context.Context, m commtypes.Message) ([]commtypes.Message, error)

type joinProcArgs struct {
	src    *processor.MeteredSource
	sink   *processor.ConcurrentMeteredSink
	wg     *sync.WaitGroup
	runner JoinWorkerFunc

	offMu         *sync.Mutex
	currentOffset map[string]uint64

	trackParFunc transaction.TrackKeySubStreamFunc

	cHashMu *sync.RWMutex
	cHash   *hash.ConsistentHash

	parNum uint8
}

func joinProc(
	ctx context.Context,
	out chan *common.FnOutput,
	procArgs *joinProcArgs,
) {
	defer procArgs.wg.Done()
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			out <- &common.FnOutput{Success: true, Message: err.Error()}
			return
		}
		out <- &common.FnOutput{Success: false, Message: err.Error()}
		return
	}
	for _, msg := range gotMsgs {
		procArgs.offMu.Lock()
		procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
		procArgs.offMu.Unlock()

		st := msg.Msg.Value.(commtypes.StreamTimeExtractor)
		ts, err := st.ExtractStreamTime()
		if err != nil {
			debug.Fprintf(os.Stderr, "%s return extract ts err: %v\n", ctx.Value("id"), err)
			out <- &common.FnOutput{Success: false, Message: fmt.Sprintf("fail to extract timestamp: %v", err)}
			return
		}
		msg.Msg.Timestamp = ts
		msgs, err := procArgs.runner(ctx, msg.Msg)
		if err != nil {
			debug.Fprintf(os.Stderr, "%s return runner err: %v\n", ctx.Value("id"), err)
			out <- &common.FnOutput{Success: false, Message: err.Error()}
		}
		err = pushMsgsToSink(ctx, procArgs.sink, procArgs.cHash, procArgs.cHashMu, msgs, procArgs.trackParFunc)
		if err != nil {
			debug.Fprintf(os.Stderr, "%s return push to sink err: %v\n", ctx.Value("id"), err)
			out <- &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	out <- nil
}

/*
func joinProcWithoutSink(
	ctx context.Context,
	out chan *common.FnOutput,
	procArgs *joinProcArgs,
) {
	defer procArgs.wg.Done()
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			out <- &common.FnOutput{Success: true, Message: err.Error()}
			return
		}
		out <- &common.FnOutput{Success: false, Message: err.Error()}
		return
	}
	for _, msg := range gotMsgs {
		procArgs.offMu.Lock()
		procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
		procArgs.offMu.Unlock()

		st := msg.Msg.Value.(commtypes.StreamTimeExtractor)
		ts, err := st.ExtractStreamTime()
		if err != nil {
			out <- &common.FnOutput{Success: false, Message: fmt.Sprintf("fail to extract timestamp: %v", err)}
			return
		}
		msg.Msg.Timestamp = ts
		_, err = procArgs.runner(ctx, msg.Msg)
		if err != nil {
			out <- &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	out <- nil
}

func joinProcSerial(
	ctx context.Context,
	procArgs *joinProcArgs,
) *common.FnOutput {
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			return &common.FnOutput{Success: true, Message: err.Error()}
		}
		return &common.FnOutput{Success: false, Message: err.Error()}
	}
	for _, msg := range gotMsgs {
		procArgs.offMu.Lock()
		procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
		procArgs.offMu.Unlock()

		st := msg.Msg.Value.(commtypes.StreamTimeExtractor)
		ts, err := st.ExtractStreamTime()
		if err != nil {
			return &common.FnOutput{Success: false, Message: fmt.Sprintf("fail to extract timestamp: %v", err)}
		}
		msg.Msg.Timestamp = ts
		msgs, err := procArgs.runner(ctx, msg.Msg)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
		err = pushMsgsToSink(ctx, procArgs.sink, procArgs.cHash, procArgs.cHashMu, msgs, procArgs.trackParFunc)
		if err != nil {
			return &common.FnOutput{Success: false, Message: err.Error()}
		}
	}
	return nil
}
*/

type joinProcWithoutSinkArgs struct {
	src    processor.Source
	runner JoinWorkerFunc
	parNum uint8
}

func joinProcSerialWithoutSink(
	ctx context.Context,
	procArgsTmp interface{},
) error {
	procArgs := procArgsTmp.(*joinProcWithoutSinkArgs)
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			return err
		}
		return err
	}
	for _, msg := range gotMsgs {
		st := msg.Msg.Value.(commtypes.StreamTimeExtractor)
		ts, err := st.ExtractStreamTime()
		if err != nil {
			return fmt.Errorf("fail to extract timestamp: %v", err)
		}
		msg.Msg.Timestamp = ts
		_, err = procArgs.runner(ctx, msg.Msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func pushMsgsToSink(
	ctx context.Context,
	sink *processor.ConcurrentMeteredSink,
	cHash *hash.ConsistentHash,
	cHashMu *sync.RWMutex,
	msgs []commtypes.Message,
	trackParFunc transaction.TrackKeySubStreamFunc,
) error {
	for _, msg := range msgs {
		key := msg.Key.(uint64)
		cHashMu.RLock()
		parTmp, ok := cHash.Get(key)
		cHashMu.RUnlock()
		if !ok {
			return fmt.Errorf("fail to calculate partition")
		}
		par := parTmp.(uint8)
		err := trackParFunc(ctx, key, sink.KeySerde(), sink.TopicName(), par)
		if err != nil {
			return fmt.Errorf("add topic partition failed: %v", err)
		}
		err = sink.Sink(ctx, msg, par, false)
		if err != nil {
			return err
		}
	}
	return nil
}

func getSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource,
	*processor.MeteredSink,
	commtypes.MsgSerde,
	error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde failed: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, err
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(sp.Duration) * time.Second,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:   commtypes.StringSerde{},
		ValueSerde: eventSerde,
		MsgSerde:   msgSerde,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, msgSerde, nil
}

func getSrcSinkUint64Key(
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource,
	*processor.MeteredSink,
	commtypes.MsgSerde,
	error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, err
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, err
	}
	inConfig := &sharedlog_stream.SharedLogStreamConfig{
		Timeout:      time.Duration(sp.Duration) * time.Second,
		MsgDecoder:   msgSerde,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:   msgSerde,
		ValueSerde: eventSerde,
		KeySerde:   commtypes.Uint64Serde{},
	}

	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig))
	sink := processor.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig))
	return src, sink, msgSerde, nil
}
