package sharedlog_stream

import (
	"context"
	"os"
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

func NewStreamPush(stream *ShardedSharedLogStream) *StreamPush {
	bufPush_str := os.Getenv("BUFPUSH")
	bufPush := false
	if bufPush_str == "true" || bufPush_str == "1" {
		bufPush = true
	}
	return &StreamPush{
		MsgChan:    make(chan PayloadToPush),
		MsgErrChan: make(chan error),
		BufPush:    bufPush,
		Stream:     stream,
	}
}

func (h *StreamPush) InitFlushTimer(duration time.Duration) {
	if h.BufPush {
		h.FlushTimer = time.NewTicker(duration)
	}
}

func (h *StreamPush) Flush(ctx context.Context) error {
	if h.BufPush {
		if h.FlushTimer != nil {
			debug.Fprintf(os.Stderr, "F: stopping flush timer\n")
			h.FlushTimer.Stop()
		}
		debug.Fprintf(os.Stderr, "F: waiting msg chan to cleanup\n")
		for len(h.MsgChan) > 0 {
			time.Sleep(time.Duration(100) * time.Microsecond)
		}
		debug.Fprintf(os.Stderr, "F: msgchan is clean\n")
		err := h.Stream.Flush(ctx)
		if err != nil {
			return err
		}
		debug.Fprintf(os.Stderr, "F: stream flushed\n")
	}
	return nil
}

func (h *StreamPush) FlushNoLock(ctx context.Context) error {
	if h.BufPush {
		if h.FlushTimer != nil {
			debug.Fprintf(os.Stderr, "FOL: stopping flush timer\n")
			h.FlushTimer.Stop()
		}
		debug.Fprintf(os.Stderr, "FOL: waiting msg chan to cleanup\n")
		for len(h.MsgChan) > 0 {
			time.Sleep(time.Duration(100) * time.Microsecond)
		}
		debug.Fprintf(os.Stderr, "FOL: msgchan is clean\n")
		err := h.Stream.FlushNoLock(ctx)
		if err != nil {
			return err
		}
		debug.Fprintf(os.Stderr, "FOL: stream flushed\n")
	}
	return nil
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

func (h *StreamPush) AsyncStreamPushNoTick(ctx context.Context, wg *sync.WaitGroup) {
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
