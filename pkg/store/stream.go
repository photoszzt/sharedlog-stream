package store

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sync"
	"time"
)

type Stream interface {
	Push(ctx context.Context, payload []byte, parNum uint8, isControl bool, payloadIsArr bool,
		taskId uint64, taskEpoch uint16, transactionID uint64) (uint64, error)
	PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64, isControl bool, payloadIsArr bool,
		taskId uint64, taskEpoch uint16, transactionID uint64) (uint64, error)
	ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg /* payload */, error)
	ReadNextWithTag(ctx context.Context, parNumber uint8, tag uint64) (*commtypes.RawMsg, error)
	ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.RawMsg, error)
	TopicName() string
	TopicNameHash() uint64
	SetCursor(cursor uint64, parNum uint8)
	NumPartition() uint8
	Flush(ctx context.Context, taskId uint64, taskEpoch uint16, transactionID uint64) error
	BufPush(ctx context.Context, payload []byte, parNum uint8, taskId uint64, taskEpoch uint16, transactionID uint64) error
}

type MeteredStream struct {
	stream Stream

	pLMu          sync.Mutex
	pushLatencies []int

	pWTLMu               sync.Mutex
	pushWithTagLatencies []int

	rNLMu             sync.Mutex
	readNextLatencies []int

	rNWTLMu                  sync.Mutex
	readNextWithTagLatencies []int

	rBWTLMu                      sync.Mutex
	readBackwardWithTagLatencies []int

	measure bool
}

var _ = Stream(&MeteredStream{})

func NewMeteredStream(stream Stream) *MeteredStream {
	measure_str := os.Getenv("MEASURE_PROC")
	measure := false
	if measure_str == "true" || measure_str == "1" {
		measure = true
	}
	return &MeteredStream{
		stream:                   stream,
		pushLatencies:            make([]int, 0, 128),
		pushWithTagLatencies:     make([]int, 0, 128),
		readNextLatencies:        make([]int, 0, 128),
		readNextWithTagLatencies: make([]int, 0, 128),
		measure:                  measure,
	}
}

func (ms *MeteredStream) Flush(ctx context.Context, taskId uint64, taskEpoch uint16, transactionID uint64) error {
	return ms.stream.Flush(ctx, taskId, taskEpoch, transactionID)
}

func (ms *MeteredStream) BufPush(ctx context.Context, payload []byte, parNum uint8, taskId uint64, taskEpoch uint16, transactionID uint64) error {
	return ms.stream.BufPush(ctx, payload, parNum, taskId, taskEpoch, transactionID)
}

func (ms *MeteredStream) Push(ctx context.Context, payload []byte, parNum uint8, isControl bool, payloadIsArr bool,
	taskId uint64, taskEpoch uint16, transactionID uint64,
) (uint64, error) {
	if ms.measure {
		procStart := time.Now()
		seq, err := ms.stream.Push(ctx, payload, parNum, isControl, payloadIsArr, taskId, taskEpoch, transactionID)
		elapsed := time.Since(procStart)

		ms.pLMu.Lock()
		ms.pushLatencies = append(ms.pushLatencies, int(elapsed.Microseconds()))
		ms.pLMu.Unlock()

		return seq, err
	}
	return ms.stream.Push(ctx, payload, parNum, isControl, payloadIsArr, taskId, taskEpoch, transactionID)
}

func (ms *MeteredStream) PushWithTag(ctx context.Context, payload []byte, parNumber uint8, tags []uint64, isControl bool, payloadIsArr bool,
	taskId uint64, taskEpoch uint16, transactionID uint64,
) (uint64, error) {
	if ms.measure {
		procStart := time.Now()
		seq, err := ms.stream.PushWithTag(ctx, payload, parNumber, tags, isControl, payloadIsArr, taskId, taskEpoch, transactionID)
		elapsed := time.Since(procStart)

		ms.pWTLMu.Lock()
		ms.pushWithTagLatencies = append(ms.pushWithTagLatencies, int(elapsed.Microseconds()))
		ms.pWTLMu.Unlock()

		return seq, err
	}
	return ms.stream.PushWithTag(ctx, payload, parNumber, tags, isControl, payloadIsArr, taskId, taskEpoch, transactionID)
}

func (ms *MeteredStream) ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg /* payload */, error) {
	if ms.measure {
		procStart := time.Now()
		rawMsg, err := ms.stream.ReadNext(ctx, parNum)
		elapsed := time.Since(procStart)

		ms.rNLMu.Lock()
		ms.readNextLatencies = append(ms.readNextLatencies, int(elapsed.Microseconds()))
		ms.rNLMu.Unlock()

		return rawMsg, err
	}
	return ms.stream.ReadNext(ctx, parNum)
}

func (ms *MeteredStream) ReadNextWithTag(ctx context.Context, parNumber uint8, tag uint64) (*commtypes.RawMsg, error) {
	if ms.measure {
		procStart := time.Now()
		rawMsg, err := ms.stream.ReadNextWithTag(ctx, parNumber, tag)
		elapsed := time.Since(procStart)

		ms.rNWTLMu.Lock()
		ms.readNextWithTagLatencies = append(ms.readNextWithTagLatencies, int(elapsed.Microseconds()))
		ms.rNWTLMu.Unlock()

		return rawMsg, err
	}
	return ms.stream.ReadNextWithTag(ctx, parNumber, tag)
}

func (ms *MeteredStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.RawMsg, error) {
	if ms.measure {
		procStart := time.Now()
		rawMsg, err := ms.stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
		elapsed := time.Since(procStart)

		ms.rBWTLMu.Lock()
		ms.readBackwardWithTagLatencies = append(ms.readBackwardWithTagLatencies, int(elapsed.Microseconds()))
		ms.rBWTLMu.Unlock()

		return rawMsg, err
	}
	return ms.stream.ReadBackwardWithTag(ctx, tailSeqNum, parNum, tag)
}

func (ms *MeteredStream) NumPartition() uint8 {
	return ms.stream.NumPartition()
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
	ms.pLMu.Lock()
	defer ms.pLMu.Unlock()
	return ms.pushLatencies
}

func (ms *MeteredStream) GetPushWithTagLatencies() []int {
	ms.pWTLMu.Lock()
	defer ms.pWTLMu.Unlock()
	return ms.pushWithTagLatencies
}

func (ms *MeteredStream) GetReadNextLatencies() []int {
	ms.rNLMu.Lock()
	defer ms.rNLMu.Unlock()
	return ms.readNextLatencies
}

func (ms *MeteredStream) GetReadNextWithTagLatencies() []int {
	ms.rNWTLMu.Lock()
	defer ms.rNWTLMu.Unlock()
	return ms.readNextWithTagLatencies
}
