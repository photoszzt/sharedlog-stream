//go:generate msgp
//msgp:ignore SharedLogStreamQueue
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

type SharedLogStreamQueue struct {
	ctx context.Context
	env types.Environment

	topicName     string
	topicNameHash uint64

	consumed   uint64
	tail       uint64
	nextSeqNum uint64
}

type StreamQueueAuxData struct {
	Consumed uint64 `msg:"consumed"`
	Tail     uint64 `msg:"tail"`
}

type StreamQueueLogEntry struct {
	auxData   *StreamQueueAuxData `msg:"-"`
	TopicName string              `msg:"topicName"`
	Payload   []byte              `msg:"payload,omitempty"`
	seqNum    uint64              `msg:"-"`
	IsPush    bool                `msg:"isPush"`
}

func streamLogTag(topicNameHash uint64) uint64 {
	return (topicNameHash << LogTagReserveBits) + StreamLogTagLowBits
}

func streamPushLogTag(topicNameHash uint64) uint64 {
	return (topicNameHash << LogTagReserveBits) + streamPushLogTagLowBits
}

func decodeStreamQueueLogEntry(logEntry *types.LogEntry) *StreamQueueLogEntry {
	streamLogEntry := &StreamQueueLogEntry{}
	_, err := streamLogEntry.UnmarshalMsg(logEntry.Data)
	if err != nil {
		panic(err)
	}
	if len(logEntry.AuxData) > 0 {
		auxData := &StreamQueueAuxData{}
		_, err := auxData.UnmarshalMsg(logEntry.AuxData)
		if err != nil {
			panic(err)
		}
		streamLogEntry.auxData = auxData
	}
	streamLogEntry.seqNum = logEntry.SeqNum
	return streamLogEntry
}

func NewSharedQueueLogStream(ctx context.Context, env types.Environment, topicName string) (*SharedLogStreamQueue, error) {
	s := &SharedLogStreamQueue{
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

func (s *SharedLogStreamQueue) TopicName() string {
	return s.topicName
}

func (s *SharedLogStreamQueue) Push(payload []byte, parNum uint8, additionalTag []uint64) (uint64, error) {
	if len(payload) == 0 {
		return 0, errEmptyPayload
	}
	logEntry := &StreamQueueLogEntry{
		TopicName: s.topicName,
		IsPush:    true,
		Payload:   payload,
	}
	encoded, err := logEntry.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	stag := streamLogTag(s.topicNameHash)
	sptag := streamPushLogTag(s.topicNameHash)
	tags := []uint64{stag, sptag}
	tags = append(tags, additionalTag...)
	seqNum, err := s.env.SharedLogAppend(s.ctx, tags, encoded)
	// fmt.Printf("Push to stream with tag %x, %x, seqNum: %x\n", stag, sptag, seqNum)

	/*
		// verify that it's appended
		if err != nil {
			return 0, err
		}
		logEntryRead, err := s.env.SharedLogReadNext(s.ctx, 0, seqNum)
		if err != nil {
			return 0, err
		}
		if logEntryRead == nil || logEntryRead.SeqNum != seqNum {
			return 0, fmt.Errorf("fail to read the log just appended")
		}
		if !bytes.Equal(encoded, logEntryRead.Data) {
			return 0, fmt.Errorf("log data mismatch")
		}
	*/
	return seqNum, err
}

func (s *SharedLogStreamQueue) isEmpty() bool {
	// fmt.Printf("consumed: %x, tail: %x\n", s.consumed, s.tail)
	return s.consumed >= s.tail
}

func (s *SharedLogStreamQueue) findNext(minSeqNum, maxSeqNum uint64) (*StreamQueueLogEntry, error) {
	tag := streamPushLogTag(s.topicNameHash)
	seqNum := minSeqNum
	// fmt.Fprintf(os.Stderr, "findNext: minSeqNum 0x%x, maxSeqNum 0x%x\n", minSeqNum, maxSeqNum)
	for seqNum < maxSeqNum {
		logEntry, err := s.env.SharedLogReadNextBlock(s.ctx, tag, seqNum)
		if err != nil {
			return nil, err
		}
		if logEntry == nil || logEntry.SeqNum >= maxSeqNum {
			return nil, nil
		}
		streamLogEntry := decodeStreamQueueLogEntry(logEntry)
		if streamLogEntry.IsPush && streamLogEntry.TopicName == s.topicName {
			// fmt.Fprintf(os.Stderr, "findNext: found entry with seqNum: 0x%x\n", logEntry.SeqNum)
			return streamLogEntry, nil
		}
		seqNum = logEntry.SeqNum + 1
	}
	return nil, nil
}

func (s *SharedLogStreamQueue) applyLog(streamLogEntry *StreamQueueLogEntry) error {
	if streamLogEntry.seqNum < s.nextSeqNum {
		log.Fatal().
			Uint64("LogSeqNum", streamLogEntry.seqNum).
			Uint64("NextSeqNum", s.nextSeqNum)
	}
	if streamLogEntry.IsPush {
		s.tail = streamLogEntry.seqNum + 1
		// fmt.Printf("Update stream tail to 0x%x with push entry\n", s.tail)
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
	// fmt.Fprintf(os.Stderr, "update stream next seq num to %x\n", s.nextSeqNum)
	return nil
}

func (s *SharedLogStreamQueue) setAuxData(seqNum uint64, auxData *StreamQueueAuxData) error {
	encoded, err := auxData.MarshalMsg(nil)
	if err != nil {
		panic(err)
	}
	return s.env.SharedLogSetAuxData(s.ctx, seqNum, encoded)
}

func (s *SharedLogStreamQueue) syncToBackward(tailSeqNum uint64) error {
	if tailSeqNum < s.nextSeqNum {
		log.Fatal().
			Uint64("Current seq", s.nextSeqNum).
			Uint64("Request seq", tailSeqNum).
			Msg("Cannot sync to request sequence number")
	}
	// fmt.Printf("tail seq is %x, current nextSeqnum is %x\n", tailSeqNum, s.nextSeqNum)
	if tailSeqNum == s.nextSeqNum {
		return nil
	}

	tag := streamLogTag(s.topicNameHash)
	streamLogs := make([]*StreamQueueLogEntry, 0, 4)

	seqNum := tailSeqNum
	for seqNum > s.nextSeqNum {
		if seqNum != protocol.MaxLogSeqnum {
			seqNum -= 1
		}
		logEntry, err := s.env.SharedLogReadPrev(s.ctx, tag, seqNum)
		if err != nil {
			return err
		}
		/*
			if logEntry != nil {
				fmt.Fprintf(os.Stderr, "cur entry seqnum: 0x%x, next seq num: 0x%x\n", logEntry.SeqNum, s.nextSeqNum)
			} else {
				fmt.Fprintf(os.Stderr, "found nil entry\n")
			}
		*/
		if logEntry == nil || logEntry.SeqNum < s.nextSeqNum {
			break
		}

		seqNum = logEntry.SeqNum
		streamLogEntry := decodeStreamQueueLogEntry(logEntry)
		// fmt.Fprintf(os.Stderr, "found tp: %v, need tp: %v\n", streamLogEntry.TopicName, s.topicName)
		if streamLogEntry.TopicName != s.topicName {
			continue
		}
		if streamLogEntry.auxData != nil {
			s.nextSeqNum = streamLogEntry.seqNum + 1
			s.consumed = streamLogEntry.auxData.Consumed
			s.tail = streamLogEntry.auxData.Tail
			// fmt.Fprintf(os.Stderr, "Update nextSeqNum to 0x%x, consumed to 0x%x, tail to 0x%x with auxData\n", s.nextSeqNum, s.consumed, s.tail)
			break
		} else {
			streamLogs = append(streamLogs, streamLogEntry)
		}
	}
	for i := len(streamLogs) - 1; i >= 0; i-- {
		streamLogEntry := streamLogs[i]
		err := s.applyLog(streamLogEntry)
		if err != nil {
			return err
		}
		auxData := &StreamQueueAuxData{
			Consumed: s.consumed,
			Tail:     s.tail,
		}
		if err := s.setAuxData(streamLogEntry.seqNum, auxData); err != nil {
			return err
		}
	}
	return nil
}

func (s *SharedLogStreamQueue) SyncToForward(tailSeqNum uint64) error {
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
		streamLogEntry := decodeStreamQueueLogEntry(logEntry)
		if streamLogEntry.TopicName == s.topicName {
			err = s.applyLog(streamLogEntry)
			if err != nil {
				return err
			}
			if streamLogEntry.auxData == nil {
				auxData := &StreamQueueAuxData{
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

func (s *SharedLogStreamQueue) syncTo(tailSeqNum uint64) error {
	return s.syncToBackward(tailSeqNum)
}

func (s *SharedLogStreamQueue) appendPopLogAndSync() error {
	logEntry := &StreamQueueLogEntry{
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

func (s *SharedLogStreamQueue) Pop(parNum uint8) ([]byte /* payload */, error) {
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

func (s *SharedLogStreamQueue) PopBlocking() ([]byte /* payload */, error) {
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
				newCtx, cancel := context.WithTimeout(s.ctx, kBlockingPopTimeout)
				defer cancel()
				logEntry, err := s.env.SharedLogReadNextBlock(newCtx, tag, seqNum)
				if err != nil {
					return nil, err
				}
				if logEntry != nil {
					streamLogEntry := decodeStreamQueueLogEntry(logEntry)
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
