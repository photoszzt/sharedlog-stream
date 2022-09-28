package sharedlog_stream

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils"
	"sync"
	"time"
)

type PayloadToPush struct {
	Payload    []byte
	Partitions []uint8
	IsControl  bool
	Mark       commtypes.EpochMark
}

type StreamPush struct {
	FlushTimer    time.Time
	MsgChan       chan PayloadToPush
	MsgErrChan    chan error
	Stream        *SizableShardedSharedLogStream
	produceCount  uint64
	ctrlCount     uint64
	FlushDuration time.Duration
	BufPush       bool
}

func NewStreamPush(stream *SizableShardedSharedLogStream) *StreamPush {
	return &StreamPush{
		MsgChan:      make(chan PayloadToPush, MSG_CHAN_SIZE),
		MsgErrChan:   make(chan error, 1),
		BufPush:      utils.CheckBufPush(),
		Stream:       stream,
		produceCount: 0,
		ctrlCount:    0,
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
func (h *StreamPush) Flush(ctx context.Context, producerId commtypes.ProducerId) error {
	if h.BufPush {
		err := h.Stream.Flush(ctx, producerId)
		if err != nil {
			return err
		}
		h.FlushTimer = time.Now()
		debug.Fprintf(os.Stderr, "F: stream flushed\n")
	}
	return nil
}

// msgchan has to close and async pusher has to stop first before calling this function
func (h *StreamPush) FlushNoLock(ctx context.Context, producerId commtypes.ProducerId) error {
	if h.BufPush {
		err := h.Stream.FlushNoLock(ctx, producerId)
		if err != nil {
			return err
		}
		h.FlushTimer = time.Now()
		debug.Fprintf(os.Stderr, "FOL: stream flushed\n")
	}
	return nil
}

func (h *StreamPush) GetCount() uint64 {
	return h.produceCount
}

func (h *StreamPush) NumCtrlMsgs() uint64 {
	return h.ctrlCount
}

func (h *StreamPush) AsyncStreamPush(ctx context.Context, wg *sync.WaitGroup, producerId commtypes.ProducerId) {
	defer wg.Done()
	for msg := range h.MsgChan {
		if msg.IsControl {
			if h.BufPush {
				err := h.Stream.Flush(ctx, producerId)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] flush err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
				h.FlushTimer = time.Now()
			}
			for _, i := range msg.Partitions {
				if msg.Mark == commtypes.SCALE_FENCE {
					fmt.Fprintf(os.Stderr, "generate scale fence for partition %d\n", i)
					scale_fence_tag := txn_data.ScaleFenceTag(h.Stream.TopicNameHash(), i)
					nameHashTag := NameHashWithPartition(h.Stream.TopicNameHash(), i)
					_, err := h.Stream.PushWithTag(ctx, msg.Payload, i, []uint64{nameHashTag, scale_fence_tag}, nil,
						StreamEntryMeta(true, false), producerId)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
						h.MsgErrChan <- err
						return
					}
					h.produceCount += 1
					h.ctrlCount += 1
				} else if msg.Mark == commtypes.STREAM_END {
					tag := NameHashWithPartition(h.Stream.TopicNameHash(), i)
					fmt.Fprintf(os.Stderr, "generate stream end mark with tag: %x\n", tag)
					_, err := h.Stream.Push(ctx, msg.Payload, i, StreamEntryMeta(true, false), producerId)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
						h.MsgErrChan <- err
						return
					}
					h.produceCount += 1
					h.ctrlCount += 1
				}
			}
		} else {
			if h.BufPush {
				timeSinceLastFlush := time.Since(h.FlushTimer)
				if timeSinceLastFlush >= h.FlushDuration {
					// debug.Fprintf(os.Stderr, "flush timer: %v\n", timeSinceLastFlush)
					err := h.Stream.Flush(ctx, producerId)
					if err != nil {
						fmt.Fprintf(os.Stderr, "[ERROR] flush err: %v\n", err)
						h.MsgErrChan <- err
						return
					}
					h.FlushTimer = time.Now()
				}
				err := h.Stream.BufPush(ctx, msg.Payload, uint8(msg.Partitions[0]), producerId)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] buf push err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
				h.produceCount += 1
			} else {
				debug.Assert(len(msg.Partitions) == 1, "should only have one partition")
				_, err := h.Stream.Push(ctx, msg.Payload, uint8(msg.Partitions[0]), StreamEntryMeta(false, false), producerId)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
				h.produceCount += 1
			}
		}
	}
}

func (h *StreamPush) AsyncStreamPushNoTick(ctx context.Context, wg *sync.WaitGroup, producerId commtypes.ProducerId,
) {
	defer wg.Done()
	for msg := range h.MsgChan {
		if msg.IsControl {
			if h.BufPush {
				err := h.Stream.Flush(ctx, producerId)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] flush err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			}
			for _, i := range msg.Partitions {
				scale_fence_tag := txn_data.ScaleFenceTag(h.Stream.TopicNameHash(), i)
				_, err := h.Stream.PushWithTag(ctx, msg.Payload, i, []uint64{scale_fence_tag}, nil,
					StreamEntryMeta(true, false), producerId)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			}
		} else {
			if h.BufPush {
				err := h.Stream.BufPushNoLock(ctx, msg.Payload, uint8(msg.Partitions[0]), producerId)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] buf flush err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			} else {
				debug.Assert(len(msg.Partitions) == 1, "should only have one partition")
				_, err := h.Stream.Push(ctx, msg.Payload, uint8(msg.Partitions[0]), SingleDataRecordMeta, producerId)
				if err != nil {
					fmt.Fprintf(os.Stderr, "[ERROR] push err: %v\n", err)
					h.MsgErrChan <- err
					return
				}
			}

		}
	}
}
