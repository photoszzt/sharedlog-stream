//go:generate msgp
//msgp:ignore SharedLogStream
package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/store"

	"github.com/rs/zerolog/log"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

type SharedLogStream struct {
	ctx           context.Context
	env           types.Environment
	topicName     string
	topicNameHash uint64

	cursor uint64
	tail   uint64
}

type StreamLogEntry struct {
	TopicName string `msg:"topicName"`
	Payload   []byte `msg:"payload,omitempty"`
	seqNum    uint64 `msg:"-"`
}

func decodeStreamLogEntry(logEntry *types.LogEntry) *StreamLogEntry {
	streamLogEntry := &StreamLogEntry{}
	_, err := streamLogEntry.UnmarshalMsg(logEntry.Data)
	if err != nil {
		panic(err)
	}
	streamLogEntry.seqNum = logEntry.SeqNum
	return streamLogEntry
}

func NewSharedLogStream(env types.Environment, topicName string) *SharedLogStream {
	return &SharedLogStream{
		env:           env,
		topicName:     topicName,
		topicNameHash: NameHash(topicName),
		cursor:        0,
		tail:          0,
	}
}

func (s *SharedLogStream) InitStream(ctx context.Context) error {
	if err := s.findLastEntryBackward(ctx, protocol.MaxLogSeqnum); err != nil {
		return err
	}
	return nil
}

func NewStream(env types.Environment, topicName string) store.Stream {
	return NewSharedLogStream(env, topicName)
}

func (s *SharedLogStream) TopicName() string {
	return s.topicName
}

func (s *SharedLogStream) Push(ctx context.Context, payload []byte, parNum uint8, additionalTag []uint64) (uint64, error) {
	if len(payload) == 0 {
		return 0, errEmptyPayload
	}
	logEntry := &StreamLogEntry{
		TopicName: s.topicName,
		Payload:   payload,
	}
	encoded, err := logEntry.MarshalMsg(nil)
	if err != nil {
		return 0, err
	}
	tags := []uint64{s.topicNameHash}
	tags = append(tags, additionalTag...)
	seqNum, err := s.env.SharedLogAppend(ctx, tags, encoded)
	s.tail = seqNum
	return seqNum, err
}

func (s *SharedLogStream) isEmpty() bool {
	return s.cursor >= s.tail
}

func (s *SharedLogStream) ReadNext(ctx context.Context, parNum uint8) ([]byte, uint64, error) {
	if s.isEmpty() {
		if err := s.findLastEntryBackward(ctx, protocol.MaxLogSeqnum); err != nil {
			return nil, 0, err
		}
		if s.isEmpty() {
			return nil, 0, errStreamEmpty
		}
	}
	tag := s.topicNameHash
	seqNum := s.cursor
	for seqNum < s.tail {
		logEntry, err := s.env.SharedLogReadNextBlock(ctx, tag, seqNum)
		if err != nil {
			return nil, 0, err
		}
		if logEntry == nil || logEntry.SeqNum >= s.tail {
			return nil, 0, errStreamEmpty
		}
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if streamLogEntry.TopicName == s.topicName {
			s.cursor = streamLogEntry.seqNum + 1
			return streamLogEntry.Payload, streamLogEntry.seqNum, nil
		}
		seqNum = logEntry.SeqNum + 1
	}
	return nil, 0, errStreamEmpty
}

func (s *SharedLogStream) findLastEntryBackward(ctx context.Context, tailSeqNum uint64) error {
	if tailSeqNum < s.cursor {
		log.Fatal().
			Uint64("Current seq", s.cursor).
			Uint64("Request seq", tailSeqNum).
			Msg("Cannot sync to request ")
	}

	if tailSeqNum == s.cursor+1 {
		return nil
	}

	tag := s.topicNameHash

	seqNum := tailSeqNum
	for seqNum > s.cursor+1 {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}

		logEntry, err := s.env.SharedLogReadPrev(s.ctx, tag, seqNum)
		if err != nil {
			return err
		}

		if logEntry != nil || logEntry.SeqNum < s.cursor+1 {
			break
		}
		seqNum = logEntry.SeqNum
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if streamLogEntry.TopicName != s.topicName {
			continue
		} else {
			s.tail = logEntry.SeqNum + 1
			break
		}
	}
	return nil
}
