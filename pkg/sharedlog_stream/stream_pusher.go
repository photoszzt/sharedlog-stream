package sharedlog_stream

import (
	"context"
	"fmt"
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
	FlushTimer    time.Time
	MsgChan       chan PayloadToPush
	MsgErrChan    chan error
	Stream        *ShardedSharedLogStream
	FlushDuration time.Duration
	BufPush       bool
}

func NewStreamPush(stream *ShardedSharedLogStream) *StreamPush {
	bufPush_str := os.Getenv("BUFPUSH")
	bufPush := false
	if bufPush_str == "true" || bufPush_str == "1" {
		bufPush = true
	}
	return &StreamPush{
		MsgChan:    make(chan PayloadToPush, MSG_CHAN_SIZE),
		MsgErrChan: make(chan error, 1),
		BufPush:    bufPush,
		Stream:     stream,
	}
}

func (h *StreamPush) InitFlushTimer(duration time.Duration) {
	if h.BufPush {
		h.FlushTimer = time.Now()
		h.FlushDuration = duration
		debug.Fprintf(os.Stderr, "InitFlushTimer: Flush duration %v\n", h.FlushDuration)
	}
}

// msgchan has to close and async pusher has to stop first before calling this function
func (h *StreamPush) Flush(ctx context.Context) error {
	if h.BufPush {
		err := h.Stream.Flush(ctx)
		if err != nil {
			return err
		}
		h.FlushTimer = time.Now()
		debug.Fprintf(os.Stderr, "F: stream flushed\n")
	}
	return nil
}

// msgchan has to close and async pusher has to stop first before calling this function
func (h *StreamPush) FlushNoLock(ctx context.Context) error {
	if h.BufPush {
		err := h.Stream.FlushNoLock(ctx)
		if err != nil {
			return err
		}
		h.FlushTimer = time.Now()
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
					fmt.Fprintf(os.Stderr, "[ERROR] flush err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
				h.FlushTimer = time.Now()
			}
			for _, i := range msg.Partitions {
				_, err := h.Stream.Push(ctx, msg.Payload, i, true, false)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			}
		} else {
			if h.BufPush {
				timeSinceLastFlush := time.Since(h.FlushTimer)
				if timeSinceLastFlush >= h.FlushDuration {
					// debug.Fprintf(os.Stderr, "flush timer: %v\n", timeSinceLastFlush)
					err := h.Stream.FlushNoLock(ctx)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] flush no lock err: %v\n", err)
						h.MsgErrChan <- err
						return
					}
					h.FlushTimer = time.Now()
				}
				err := h.Stream.BufPushNoLock(ctx, msg.Payload, uint8(msg.Partitions[0]))
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] buf push nolock err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			} else {
				debug.Assert(len(msg.Partitions) == 1, "should only have one partition")
				_, err := h.Stream.Push(ctx, msg.Payload, uint8(msg.Partitions[0]), false, false)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
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
					fmt.Fprintf(os.Stderr, "[ERROR] flush err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			}
			for _, i := range msg.Partitions {
				_, err := h.Stream.Push(ctx, msg.Payload, i, true, false)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			}
		} else {
			if h.BufPush {
				err := h.Stream.BufPushNoLock(ctx, msg.Payload, uint8(msg.Partitions[0]))
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] buf flush err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			} else {
				debug.Assert(len(msg.Partitions) == 1, "should only have one partition")
				_, err := h.Stream.Push(ctx, msg.Payload, uint8(msg.Partitions[0]), false, false)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			}

		}
	}
}
