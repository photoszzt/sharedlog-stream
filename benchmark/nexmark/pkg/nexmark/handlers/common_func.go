package handlers

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sync"
	"sync/atomic"
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

/*
func getPersonTimeSerde(serdeFormat uint8) (commtypes.Serde, error) {
	if serdeFormat == uint8(commtypes.JSON) {
		return ntypes.PersonTimeJSONSerde{}, nil
	} else if serdeFormat == uint8(commtypes.MSGP) {
		return ntypes.PersonTimeMsgpSerde{}, nil
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", serdeFormat)
	}
}
*/

type JoinWorkerFunc func(c context.Context, m commtypes.Message) ([]commtypes.Message, error)

type joinProcArgs struct {
	src           *processor.MeteredSource
	sink          *sharedlog_stream.ConcurrentMeteredSink
	currentOffset map[string]uint64
	trackParFunc  transaction.TrackKeySubStreamFunc
	cHash         *hash.ConsistentHash
	runner        JoinWorkerFunc
	wg            sync.WaitGroup
	offMu         *sync.Mutex
	cHashMu       *sync.RWMutex
	parNum        uint8
}

func joinProc(
	ctx context.Context,
	out chan *common.FnOutput,
	procArgs *joinProcArgs,
	done *uint32,
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
	for _, msg := range gotMsgs.Msgs {
		procArgs.offMu.Lock()
		procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
		procArgs.offMu.Unlock()

		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				if subMsg.Value == nil {
					continue
				}
				err = procMsgWithSink(ctx, subMsg, procArgs)
				if err != nil {
					atomic.StoreUint32(done, 1)
					out <- &common.FnOutput{Success: false, Message: err.Error()}
				}
			}
		} else {
			if msg.Msg.Value == nil {
				continue
			}
			err = procMsgWithSink(ctx, msg.Msg, procArgs)
			if err != nil {
				atomic.StoreUint32(done, 1)
				out <- &common.FnOutput{Success: false, Message: err.Error()}
			}
		}
	}
	out <- nil
}

func joinProcLoop(
	ctx context.Context,
	out chan *common.FnOutput,
	procArgs *joinProcArgs,
	wg *sync.WaitGroup,
	run chan struct{},
	done chan struct{},
) {
	<-run
	debug.Fprintf(os.Stderr, "joinProc start running\n")
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			debug.Fprintf(os.Stderr, "got done msg\n")
			return
		default:
		}
		// debug.Fprintf(os.Stderr, "before consume\n")
		gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
		if err != nil {
			if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
				debug.Fprintf(os.Stderr, "%s timeout\n", procArgs.src.TopicName())
				out <- &common.FnOutput{Success: true, Message: err.Error()}
				// debug.Fprintf(os.Stderr, "Done sending result\n")
				return
			}
			debug.Fprintf(os.Stderr, "[ERROR] consume: %v\n", err)
			out <- &common.FnOutput{Success: false, Message: err.Error()}
			return
		}
		// debug.Fprintf(os.Stderr, "after consume\n")
		for _, msg := range gotMsgs.Msgs {
			procArgs.offMu.Lock()
			procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
			procArgs.offMu.Unlock()

			if msg.MsgArr != nil {
				for _, subMsg := range msg.MsgArr {
					if subMsg.Value == nil {
						continue
					}
					err = procMsgWithSink(ctx, subMsg, procArgs)
					if err != nil {
						debug.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink: %v\n", ctx.Value("id").(string), err)
						out <- &common.FnOutput{Success: false, Message: err.Error()}
						debug.Fprintf(os.Stderr, "done send msg\n", err)
						return
					}
				}
			} else {
				if msg.Msg.Value == nil {
					continue
				}
				err = procMsgWithSink(ctx, msg.Msg, procArgs)
				if err != nil {
					debug.Fprintf(os.Stderr, "[ERROR] %s progMsgWithSink: %v\n", ctx.Value("id").(string), err)
					out <- &common.FnOutput{Success: false, Message: err.Error()}
					return
				}
			}
		}
		// debug.Fprintf(os.Stderr, "after for loop\n")
	}
}

func procMsgWithSink(ctx context.Context, msg commtypes.Message, procArgs *joinProcArgs,
) error {
	st := msg.Value.(commtypes.StreamTimeExtractor)
	ts, err := st.ExtractStreamTime()
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return extract ts: %v\n", ctx.Value("id"), err)
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	msgs, err := procArgs.runner(ctx, msg)
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return runner: %v\n", ctx.Value("id"), err)
		return err
	}
	err = pushMsgsToSink(ctx, procArgs.sink, procArgs.cHash, procArgs.cHashMu, msgs, procArgs.trackParFunc)
	if err != nil {
		debug.Fprintf(os.Stderr, "[ERROR] %s return push to sink: %v\n", ctx.Value("id"), err)
		return err
	}
	return nil
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
	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				err = procMsg(ctx, subMsg, procArgs)
				if err != nil {
					return err
				}
			}
		} else {
			err = procMsg(ctx, msg.Msg, procArgs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func procMsg(ctx context.Context, msg commtypes.Message, procArgs *joinProcWithoutSinkArgs) error {
	st := msg.Value.(commtypes.StreamTimeExtractor)
	ts, err := st.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	_, err = procArgs.runner(ctx, msg)
	if err != nil {
		return err
	}
	return nil
}

func pushMsgsToSink(
	ctx context.Context,
	sink *sharedlog_stream.ConcurrentMeteredSink,
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
	*sharedlog_stream.MeteredSink,
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
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:      commtypes.StringSerde{},
		ValueSerde:    eventSerde,
		MsgSerde:      msgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig), time.Duration(sp.WarmupS)*time.Second)
	sink := sharedlog_stream.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig), time.Duration(sp.WarmupS)*time.Second)
	return src, sink, msgSerde, nil
}

func getSrcSinkUint64Key(
	ctx context.Context,
	sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource,
	*sharedlog_stream.MeteredSink,
	commtypes.MsgSerde,
	error,
) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, err
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, err
	}
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		MsgDecoder:   msgSerde,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		MsgSerde:      msgSerde,
		ValueSerde:    eventSerde,
		KeySerde:      commtypes.Uint64Serde{},
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}

	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := sharedlog_stream.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, msgSerde, nil
}

func CommonGetSrcSink(ctx context.Context, sp *common.QueryInput,
	input_stream *sharedlog_stream.ShardedSharedLogStream,
	output_stream *sharedlog_stream.ShardedSharedLogStream,
) (*processor.MeteredSource, *sharedlog_stream.MeteredSink, commtypes.MsgSerde, error) {
	msgSerde, err := commtypes.GetMsgSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get msg serde err: %v", err)
	}
	eventSerde, err := getEventSerde(sp.SerdeFormat)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get event serde err: %v", err)
	}
	inConfig := &sharedlog_stream.StreamSourceConfig{
		Timeout:      common.SrcConsumeTimeout,
		KeyDecoder:   commtypes.StringDecoder{},
		ValueDecoder: eventSerde,
		MsgDecoder:   msgSerde,
	}
	outConfig := &sharedlog_stream.StreamSinkConfig{
		KeySerde:      commtypes.Uint64Serde{},
		ValueSerde:    eventSerde,
		MsgSerde:      msgSerde,
		FlushDuration: time.Duration(sp.FlushMs) * time.Millisecond,
	}
	// fmt.Fprintf(os.Stderr, "output to %v\n", output_stream.TopicName())
	src := processor.NewMeteredSource(sharedlog_stream.NewShardedSharedLogStreamSource(input_stream, inConfig),
		time.Duration(sp.WarmupS)*time.Second)
	sink := sharedlog_stream.NewMeteredSink(sharedlog_stream.NewShardedSharedLogStreamSink(output_stream, outConfig),
		time.Duration(sp.WarmupS)*time.Second)
	return src, sink, msgSerde, nil
}
