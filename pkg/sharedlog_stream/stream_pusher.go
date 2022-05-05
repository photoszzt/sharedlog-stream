package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/debug"
	"sync"
	"time"
)

type PayloadToPush struct {
	Payload    []byte
	Partitions []uint8
	IsControl  bool
}

type StreamPush struct {
	FlushTimer *time.Ticker
	MsgChan    chan PayloadToPush
	MsgErrChan chan error
	Stream     *ShardedSharedLogStream
	BufPush    bool
}

func (h *StreamPush) AsyncStreamPush(ctx context.Context, wg *sync.WaitGroup,
) {
	defer wg.Done()
	for msg := range h.MsgChan {
		if msg.IsControl {
			if h.BufPush {
				err := h.Stream.Flush(ctx)
				if err != nil {
					h.MsgErrChan <- err
					return
				}
			}
			for _, i := range msg.Partitions {
				_, err := h.Stream.Push(ctx, msg.Payload, i, true, false)
				if err != nil {
					h.MsgErrChan <- err
					return
				}
			}
		} else {
			if h.BufPush {
				select {
				case <-h.FlushTimer.C:
					h.Stream.FlushNoLock(ctx)
				default:
				}
				err := h.Stream.BufPushNoLock(ctx, msg.Payload, uint8(msg.Partitions[0]))
				if err != nil {
					h.MsgErrChan <- err
					return
				}
			} else {
				debug.Assert(len(msg.Partitions) == 1, "should only have one partition")
				_, err := h.Stream.Push(ctx, msg.Payload, uint8(msg.Partitions[0]), false, false)
				if err != nil {
					h.MsgErrChan <- err
					return
				}
			}

		}
	}
}
