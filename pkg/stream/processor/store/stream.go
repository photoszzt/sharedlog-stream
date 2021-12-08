package store

import (
	"context"
	"os"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type Stream interface {
	Push(ctx context.Context, payload []byte, parNum uint8, isControl bool) (uint64, error)
	PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64, isControl bool) (uint64, error)
	ReadNext(ctx context.Context, parNum uint8) (commtypes.TaskIDGen, []commtypes.RawMsg /* payload */, error)
	ReadNextWithTag(ctx context.Context, parNumber uint8, tag uint64) (commtypes.TaskIDGen, []commtypes.RawMsg, error)
	ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.TaskIDGen, *commtypes.RawMsg, error)
	TopicName() string
	TopicNameHash() uint64
	SetCursor(cursor uint64, parNum uint8)
}

type MeteredStream struct {
	stream                       Stream
	pushLatencies                []int
	pushWithTagLatencies         []int
	readNextLatencies            []int
	readNextWithTagLatencies     []int
	readBackwardWithTagLatencies []int
}

var _ = Stream(&MeteredStream{})

func NewMeteredStream(stream Stream) *MeteredStream {
	return &MeteredStream{
		stream:                   stream,
		pushLatencies:            make([]int, 0, 128),
		pushWithTagLatencies:     make([]int, 0, 128),
		readNextLatencies:        make([]int, 0, 128),
		readNextWithTagLatencies: make([]int, 0, 128),
	}
}

func (ms *MeteredStream) Push(ctx context.Context, payload []byte, parNum uint8, isControl bool) (uint64, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		seq, err := ms.stream.Push(ctx, payload, parNum, isControl)
		elapsed := time.Since(procStart)
		ms.pushLatencies = append(ms.pushLatencies, int(elapsed.Microseconds()))
		return seq, err
	}
	return ms.stream.Push(ctx, payload, parNum, isControl)
}

func (ms *MeteredStream) PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64, isControl bool) (uint64, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		seq, err := ms.stream.PushWithTag(ctx, payload, parNumber, tags, isControl)
		elapsed := time.Since(procStart)
		ms.pushWithTagLatencies = append(ms.pushWithTagLatencies, int(elapsed.Microseconds()))
		return seq, err
	}
	return ms.stream.PushWithTag(ctx, payload, parNumber, tags, isControl)
}

func (ms *MeteredStream) ReadNext(ctx context.Context, parNum uint8) (commtypes.TaskIDGen, []commtypes.RawMsg /* payload */, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		appIdGen, rawMsgs, err := ms.stream.ReadNext(ctx, parNum)
		elapsed := time.Since(procStart)
		ms.readNextLatencies = append(ms.readNextLatencies, int(elapsed.Microseconds()))
		return appIdGen, rawMsgs, err
	}
	return ms.stream.ReadNext(ctx, parNum)
}

func (ms *MeteredStream) ReadNextWithTag(ctx context.Context, parNumber uint8, tag uint64) (commtypes.TaskIDGen, []commtypes.RawMsg, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		appIdGen, rawMsgs, err := ms.stream.ReadNextWithTag(ctx, parNumber, tag)
		elapsed := time.Since(procStart)
		ms.readNextWithTagLatencies = append(ms.readNextWithTagLatencies, int(elapsed.Microseconds()))
		return appIdGen, rawMsgs, err
	}
	return ms.stream.ReadNextWithTag(ctx, parNumber, tag)
}

func (ms *MeteredStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.TaskIDGen, *commtypes.RawMsg, error) {
	measure_proc := os.Getenv("MEASURE_PROC")
	if measure_proc == "true" || measure_proc == "1" {
		procStart := time.Now()
		appIdGen, rawMsg, err := ms.stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
		elapsed := time.Since(procStart)
		ms.readBackwardWithTagLatencies = append(ms.readBackwardWithTagLatencies, int(elapsed.Microseconds()))
		return appIdGen, rawMsg, err
	}
	return ms.stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
}

func (ms *MeteredStream) TopicName() string {
	return ms.stream.TopicName()
}

func (ms *MeteredStream) TopicNameHash() uint64 {
	return ms.stream.TopicNameHash()
}

func (ms *MeteredStream) SetCursor(cursor uint64, parNum uint8) {
	ms.stream.SetCursor(cursor, parNum)
}

func (ms *MeteredStream) GetPushLatencies() []int {
	return ms.pushLatencies
}

func (ms *MeteredStream) GetPushWithTagLatencies() []int {
	return ms.pushWithTagLatencies
}

func (ms *MeteredStream) GetReadNextLatencies() []int {
	return ms.readNextLatencies
}

func (ms *MeteredStream) GetReadNextWithTagLatencies() []int {
	return ms.readNextWithTagLatencies
}
