//go:generate msgp
package sharedlog_stream

import (
	"context"
	"errors"
	"time"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
)

var (
	errEmptyPayload  = errors.New("payload cannot be empty")
	errStreamEmpty   = errors.New("stream empty")
	errStreamTimeout = errors.New("blocking pop timeout")
)

const (
	kBlockingPopTimeout = 1 * time.Second
)

type SharedLogStream struct {
	ctx context.Context
	env types.Environment

	topicName     string
	topicNameHash uint64

	consumed   uint64
	tail       uint64
	nextSeqNum uint64
}

type StreamAuxData struct {
	Consumed uint64 `msg:"h"`
	Tail     uint64 `msg:"t"`
}

type StreamLogEntry struct {
	seqNum  uint64
	auxData *StreamAuxData

	TopicName string `msg:"n"`
	IsPush    bool   `msg:"t"`
	Payload   []byte `msg:"p,omitempty"`
}

func streamLogTag(topicNameHash uint64) uint64 {
	return (topicNameHash << LogTagReserveBits) + StreamLogTagLowBits
}

func streamPushLogTag(topicNameHash uint64) uint64 {
	return (topicNameHash << LogTagReserveBits) + streamPushLogTagLowBits
}

func decodeStreamLogEntry(logEntry *types.LogEntry) *StreamLogEntry {
	streamLogEntry := &StreamLogEntry{}
	_, err := streamLogEntry.UnmarshalMsg(logEntry.Data)
	if err != nil {
		panic(err)
	}
	if len(logEntry.AuxData) > 0 {
		auxData := &StreamAuxData{}
		_, err := auxData.UnmarshalMsg(logEntry.AuxData)
		if err != nil {
			panic(err)
		}
		streamLogEntry.auxData = auxData
	}
	streamLogEntry.seqNum = logEntry.SeqNum
	return streamLogEntry
}

func NewSharedLogStream(ctx context.Context, env types.Environment, topicName string) (*SharedLogStream, error) {
	s := &SharedLogStream{
		ctx:           ctx,
		env:           env,
		topicName:     topicName,
		topicNameHash: NameHash(topicName),
		consumed:      0,
		tail:          0,
		nextSeqNum:    0,
	}
	if err := s.syncToBackward(protocol.MaxLogSeqnum); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *SharedLogStream) Push(payload []byte) error {
	if len(payload) == 0 {
		return errEmptyPayload
	}
	logEntry := &StreamLogEntry{
		TopicName: s.topicName,
		IsPush:    true,
		Payload:   payload,
	}
	encoded, err := logEntry.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	tags := []uint64{streamLogTag(s.topicNameHash), streamPushLogTag(s.topicNameHash)}
	_, err = s.env.SharedLogAppend(s.ctx, tags, encoded)
	return err
}

func (s *SharedLogStream) isEmpty() bool {
	return s.consumed >= s.tail
}

func (s *SharedLogStream) findNext(minSeqNum, maxSeqNum uint64) (*StreamLogEntry, error) {
	tag := streamPushLogTag(s.topicNameHash)
	seqNum := minSeqNum
	for seqNum < maxSeqNum {
		logEntry, err := s.env.SharedLogReadNextBlock(s.ctx, tag, seqNum)
		if err != nil {
			return nil, err
		}
		if logEntry == nil || logEntry.SeqNum >= maxSeqNum {
			return nil, nil
		}
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if streamLogEntry.IsPush && streamLogEntry.TopicName == s.topicName {
			return streamLogEntry, nil
		}
		seqNum = logEntry.SeqNum + 1
	}
	return nil, nil
}

func (s *SharedLogStream) applyLog(streamLogEntry *StreamLogEntry) error {
	if streamLogEntry.seqNum < s.nextSeqNum {
		log.Fatal().
			Uint64("LogSeqNum", streamLogEntry.seqNum).
			Uint64("NextSeqNum", s.nextSeqNum)
	}
	if streamLogEntry.IsPush {
		s.tail = streamLogEntry.seqNum + 1
	} else {
		nextLog, err := s.findNext(s.consumed, s.tail)
		if err != nil {
			return err
		}
		if nextLog != nil {
			s.consumed = nextLog.seqNum + 1
		} else {
			s.consumed = streamLogEntry.seqNum
		}
	}
	s.nextSeqNum = streamLogEntry.seqNum + 1
	return nil
}

func (s *SharedLogStream) setAuxData(seqNum uint64, auxData *StreamAuxData) error {
	encoded, err := auxData.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	return s.env.SharedLogSetAuxData(s.ctx, seqNum, encoded)
}

func (s *SharedLogStream) syncToBackward(tailSeqNum uint64) error {
	if tailSeqNum < s.nextSeqNum {
		log.Fatal().
			Uint64("Current seq", s.nextSeqNum).
			Uint64("Request seq", tailSeqNum).
			Msg("Cannot sync to request sequence number")
	}
	if tailSeqNum == s.nextSeqNum {
		return nil
	}

	tag := streamLogTag(s.topicNameHash)
	streamLogs := make([]*StreamLogEntry, 0, 4)

	seqNum := tailSeqNum
	for seqNum > s.nextSeqNum {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}
		logEntry, err := s.env.SharedLogReadPrev(s.ctx, tag, seqNum)
		if err != nil {
			return err
		}
		if logEntry == nil || logEntry.SeqNum < s.nextSeqNum {
			break
		}
		seqNum = logEntry.SeqNum
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if streamLogEntry.TopicName != s.topicName {
			continue
		}
		if streamLogEntry.auxData != nil {
			s.nextSeqNum = streamLogEntry.seqNum + 1
			s.consumed = streamLogEntry.auxData.Consumed
			s.tail = streamLogEntry.auxData.Tail
			break
		} else {
			streamLogs = append(streamLogs, streamLogEntry)
		}
	}
	for i := len(streamLogs) - 1; i >= 0; i-- {
		streamLogEntry := streamLogs[i]
		s.applyLog(streamLogEntry)
		auxData := &StreamAuxData{
			Consumed: s.consumed,
			Tail:     s.tail,
		}
		if err := s.setAuxData(streamLogEntry.seqNum, auxData); err != nil {
			return err
		}
	}
	return nil
}

func (s *SharedLogStream) syncToForward(tailSeqNum uint64) error {
	if tailSeqNum < s.nextSeqNum {
		log.Fatal().
			Uint64("Current seqNum", s.nextSeqNum).
			Uint64("Request seqNum", tailSeqNum).
			Msg("Cannot sync to request seqNum")
	}
	tag := streamLogTag(s.topicNameHash)
	seqNum := s.nextSeqNum
	for seqNum < tailSeqNum {
		logEntry, err := s.env.SharedLogReadNext(s.ctx, tag, seqNum)
		if err != nil {
			return err
		}
		if logEntry == nil || logEntry.SeqNum >= tailSeqNum {
			break
		}
		seqNum = logEntry.SeqNum + 1
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if streamLogEntry.TopicName == s.topicName {
			s.applyLog(streamLogEntry)
			if streamLogEntry.auxData == nil {
				auxData := &StreamAuxData{
					Consumed: s.consumed,
					Tail:     s.tail,
				}
				if err := s.setAuxData(streamLogEntry.seqNum, auxData); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func (s *SharedLogStream) syncTo(tailSeqNum uint64) error {
	return s.syncToBackward(tailSeqNum)
}

func (s *SharedLogStream) appendPopLogAndSync() error {
	logEntry := &StreamLogEntry{
		TopicName: s.topicName,
		IsPush:    false,
	}
	encoded, err := logEntry.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	tags := []uint64{streamLogTag(s.topicNameHash)}
	if seqNum, err := s.env.SharedLogAppend(s.ctx, tags, encoded); err != nil {
		return err
	} else {
		return s.syncTo(seqNum)
	}
}

func IsStreamEmptyError(err error) bool {
	return err == errStreamEmpty
}

func IsStreamTimeoutError(err error) bool {
	return err == errStreamTimeout
}

func (s *SharedLogStream) Pop() ([]byte /* payload */, error) {
	if s.isEmpty() {
		if err := s.syncTo(protocol.MaxLogSeqnum); err != nil {
			return nil, err
		}
		if s.isEmpty() {
			return nil, errStreamEmpty
		}
	}
	if err := s.appendPopLogAndSync(); err != nil {
		return nil, err
	}
	if nextLog, err := s.findNext(s.consumed, s.tail); err != nil {
		return nil, err
	} else if nextLog != nil {
		return nextLog.Payload, nil
	} else {
		return nil, errStreamEmpty
	}
}

func (s *SharedLogStream) PopBlocking() ([]byte /* payload */, error) {
	tag := streamPushLogTag(s.topicNameHash)
	startTime := time.Now()
	for time.Since(startTime) < kBlockingPopTimeout {
		if s.isEmpty() {
			if err := s.syncTo(protocol.MaxLogSeqnum); err != nil {
				return nil, err
			}
		}
		if s.isEmpty() {
			seqNum := s.nextSeqNum
			for {
				newCtx, _ := context.WithTimeout(s.ctx, kBlockingPopTimeout)
				logEntry, err := s.env.SharedLogReadNextBlock(newCtx, tag, seqNum)
				if err != nil {
					return nil, err
				}
				if logEntry != nil {
					streamLogEntry := decodeStreamLogEntry(logEntry)
					if streamLogEntry.IsPush && streamLogEntry.TopicName == s.topicName {
						break
					}
					seqNum = logEntry.SeqNum + 1
				} else if time.Since(startTime) >= kBlockingPopTimeout {
					return nil, errStreamTimeout
				}
			}
		}
		if err := s.appendPopLogAndSync(); err != nil {
			return nil, err
		}
		if nextLog, err := s.findNext(s.consumed, s.tail); err != nil {
			return nil, err
		} else if nextLog != nil {
			return nextLog.Payload, nil
		}
	}
	return nil, errStreamTimeout
}