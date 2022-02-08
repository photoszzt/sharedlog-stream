package handlers

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/benchmark/common"
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"
	"sync"

	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"

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

type JoinWorkerFunc func(c context.Context, m commtypes.Message, sink *processor.MeteredSink, trackParFunc func([]uint8) error) error

type joinProcArgs struct {
	src           *processor.MeteredSource
	sink          *processor.MeteredSink
	wg            *sync.WaitGroup
	runner        JoinWorkerFunc
	offMu         *sync.Mutex
	currentOffset map[string]uint64
	trackParFunc  func([]uint8) error
	parNum        uint8
}

func joinProc(
	ctx context.Context,
	out chan *common.FnOutput,
	procArgs *joinProcArgs,
) {
	defer procArgs.wg.Done()
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		fmt.Fprint(os.Stderr, "got error\n")
		if xerrors.Is(err, sharedlog_stream.ErrStreamSourceTimeout) {
			fmt.Fprint(os.Stderr, "timeout, gen output\n")
			out <- &common.FnOutput{
				Success: true,
				Message: err.Error(),
			}
			return
		}
		out <- &common.FnOutput{
			Success: false,
			Message: err.Error(),
		}
		return
	}
	for _, msg := range gotMsgs {
		procArgs.offMu.Lock()
		procArgs.currentOffset[procArgs.src.TopicName()] = msg.LogSeqNum
		procArgs.offMu.Unlock()

		event := msg.Msg.Value.(*ntypes.Event)
		ts, err := event.ExtractStreamTime()
		if err != nil {
			out <- &common.FnOutput{
				Success: false,
				Message: fmt.Sprintf("fail to extract timestamp: %v", err),
			}
			return
		}
		msg.Msg.Timestamp = ts
		err = procArgs.runner(ctx, msg.Msg, procArgs.sink, procArgs.trackParFunc)
		if err != nil {
			out <- &common.FnOutput{
				Success: false,
				Message: err.Error(),
			}
		}
	}
	fmt.Fprint(os.Stderr, "output nil\n")
	out <- nil
	fmt.Fprint(os.Stderr, "done output nil\n")
}

func pushMsgsToSink(
	ctx context.Context,
	sink *processor.MeteredSink,
	cHash *hash.ConsistentHash,
	msgs []commtypes.Message,
	trackParFunc func([]uint8) error,
) error {
	for _, msg := range msgs {
		key := msg.Key.(uint64)
		parTmp, ok := cHash.Get(key)
		if !ok {
			return fmt.Errorf("fail to calculate partition")
		}
		par := parTmp.(uint8)
		err := trackParFunc([]uint8{par})
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
