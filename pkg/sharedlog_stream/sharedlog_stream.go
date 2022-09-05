//go:generate msgp
//msgp:ignore SharedLogStream
package sharedlog_stream

import (
	"context"
	"fmt"
	"math"
	"sharedlog-stream/pkg/bits"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/hashfuncs"
	"sharedlog-stream/pkg/txn_data"
	"sharedlog-stream/pkg/utils/syncutils"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

const (
	kBlockingReadTimeout = 1 * time.Second
)

type LogEntryMeta uint8

const (
	Control bits.Bits = 1 << iota
	PayloadArr
)

func StreamEntryMeta(isControl bool, payloadIsArr bool) LogEntryMeta {
	var meta bits.Bits
	if isControl {
		meta = bits.Set(meta, Control)
	}
	if payloadIsArr {
		meta = bits.Set(meta, PayloadArr)
	}
	return LogEntryMeta(meta)
}

var SingleDataRecordMeta = StreamEntryMeta(false, false)
var ControlRecordMeta = StreamEntryMeta(true, false)
var ArrRecordMeta = StreamEntryMeta(false, true)

type SharedLogStream struct {
	mux syncutils.RWMutex
	env types.Environment
	// txnMarkerSerde commtypes.Serde
	topicName     string
	topicNameHash uint64
	// current read position in forward direction
	cursor             uint64
	tail               uint64 // protected by mux
	curAppendMsgSeqNum uint64
}

func NameHashWithPartition(nameHash uint64, par uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<txn_data.PartitionBits - 1)
	return (nameHash & mask) + uint64(par)
}

type StreamLogEntry struct {
	TopicName     []string `msg:"topicName"`
	Payload       []byte   `msg:"payload,omitempty"`
	TaskId        uint64   `msg:"tid,omitempty"`
	MsgSeqNum     uint64   `msg:"mseq,omitempty"`
	TransactionID uint64   `msg:"trid,omitempty"`
	TaskEpoch     uint16   `msg:"te,omitempty"`
	Meta          uint8    `msg:"meta,omitempty"`
}

func (e *StreamLogEntry) BelongsToTopic(topicName string) bool {
	// for common case where an entry only belongs to one topic
	if e.TopicName[0] == topicName {
		return true
	} else {
		// for mark record, it can be read by multiple topics
		for i := 1; i < len(e.TopicName); i++ {
			if e.TopicName[i] == topicName {
				return true
			}
		}
		return false
	}
}

func decodeStreamLogEntry(logEntry *types.LogEntry) *StreamLogEntry {
	streamLogEntry := StreamLogEntry{}
	_, err := streamLogEntry.UnmarshalMsg(logEntry.Data)
	if err != nil {
		panic(err)
	}
	return &streamLogEntry
}

func NewSharedLogStream(env types.Environment, topicName string, serdeFormat commtypes.SerdeFormat) (*SharedLogStream, error) {
	return &SharedLogStream{
		env:           env,
		topicName:     topicName,
		topicNameHash: hashfuncs.NameHash(topicName),
		cursor:        0,
		tail:          0,

		curAppendMsgSeqNum: 0,
	}, nil
}

func (s *SharedLogStream) NumPartition() uint8 {
	return 1
}

func (s *SharedLogStream) SetAppendMsgSeqNum(val uint64) {
	s.curAppendMsgSeqNum = val
}

func (s *SharedLogStream) TopicNameHash() uint64 {
	return s.topicNameHash
}

func (s *SharedLogStream) TopicName() string {
	return s.topicName
}

func (s *SharedLogStream) SetCursor(cursor uint64, parNum uint8) {
	s.cursor = cursor
}

// multiple thread could push to the same stream but only one reader
func (s *SharedLogStream) PushWithTag(ctx context.Context,
	payload []byte, parNum uint8, tags []uint64, additionalTopicNames []string,
	meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	if len(payload) == 0 {
		return 0, common_errors.ErrEmptyPayload
	}
	topics := []string{s.topicName}
	if additionalTopicNames != nil {
		topics = append(topics, additionalTopicNames...)
	}
	logEntry := &StreamLogEntry{
		TopicName: topics,
		Payload:   payload,
		Meta:      uint8(meta),
	}
	// if s.inTransaction {
	// TODO: need to deal with sequence number overflow
	atomic.AddUint64(&s.curAppendMsgSeqNum, 1)
	logEntry.MsgSeqNum = s.curAppendMsgSeqNum
	logEntry.TaskEpoch = producerId.TaskEpoch
	logEntry.TaskId = producerId.TaskId
	logEntry.TransactionID = producerId.TransactionID
	// }
	encoded, err := logEntry.MarshalMsg(nil)
	if err != nil {
		return 0, err
	}

	s.mux.Lock()
	seqNum, err := s.env.SharedLogAppend(ctx, tags, encoded)
	if err != nil {
		return 0, err
	}
	s.tail = seqNum + 1
	s.mux.Unlock()

	// verify that push is successful

	// debug.Fprintf(os.Stderr, "push to %s tag: ", s.topicName)
	// for _, t := range tags {
	// 	debug.Fprintf(os.Stderr, "0x%x ", t)
	// }
	// debug.Fprintf(os.Stderr, "seqNum 0x%x, tail 0x%x\n", seqNum, s.tail)
	/*
		// debug.Fprintf(os.Stderr, "append val %s with tag: %x to topic %s par %d, seqNum: %x\n",
		// 	string(payload), tags[0], s.topicName, parNum, seqNum)
		logEntryRead, err := s.env.SharedLogReadNext(ctx, tags[0], seqNum)
		if err != nil {
			return 0, err
		}
		if logEntryRead == nil || logEntryRead.SeqNum != seqNum {
			return 0, fmt.Errorf("fail to read the log just appended")
		}
		if !bytes.Equal(encoded, logEntryRead.Data) {
			return 0, fmt.Errorf("log data mismatch")
		}

		logEntryRead, err = s.env.SharedLogReadPrev(ctx, tags[0], seqNum+1)
		if err != nil {
			return 0, err
		}
		if logEntryRead == nil || logEntryRead.SeqNum != seqNum {
			return 0, fmt.Errorf("fail to read the log just appended from backward")
		}

		if !bytes.Equal(encoded, logEntryRead.Data) {
			return 0, fmt.Errorf("log data mismatch")
		}
	*/

	return seqNum, nil
}

func (s *SharedLogStream) Push(ctx context.Context, payload []byte, parNum uint8, meta LogEntryMeta, producerId commtypes.ProducerId,
) (uint64, error) {
	tags := []uint64{NameHashWithPartition(s.topicNameHash, parNum)}
	return s.PushWithTag(ctx, payload, parNum, tags, nil, meta, producerId)
}

func (s *SharedLogStream) isEmpty() bool {
	s.mux.RLock()
	ret := s.cursor >= s.tail
	s.mux.RUnlock()
	return ret
}

func (s *SharedLogStream) ReadBackwardWithTag(ctx context.Context, tailSeqNum uint64, parNum uint8, tag uint64) (*commtypes.RawMsg, error) {
	seqNum := tailSeqNum
	for {
		logEntry, err := s.readPrevWithTimeout(ctx, tag, seqNum)
		if err != nil {
			return nil, err
		}
		if logEntry == nil {
			break
		}
		seqNum = logEntry.SeqNum
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if !streamLogEntry.BelongsToTopic(s.topicName) {
			continue
		} else {
			isControl := bits.Has(bits.Bits(streamLogEntry.Meta), Control)
			isPayloadArr := bits.Has(bits.Bits(streamLogEntry.Meta), PayloadArr)
			return &commtypes.RawMsg{
				Payload:      streamLogEntry.Payload,
				AuxData:      logEntry.AuxData,
				MsgSeqNum:    streamLogEntry.MsgSeqNum,
				LogSeqNum:    logEntry.SeqNum,
				IsControl:    isControl,
				IsPayloadArr: isPayloadArr,
			}, nil
		}
	}
	return nil, common_errors.ErrStreamEmpty
}

func (s *SharedLogStream) ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	tag := NameHashWithPartition(s.topicNameHash, parNum)
	return s.ReadNextWithTag(ctx, parNum, tag)
}

func (s *SharedLogStream) ReadNextWithTag(ctx context.Context, parNum uint8, tag uint64) (*commtypes.RawMsg, error) {
	// debug.Fprintf(os.Stderr, "read topic %s with parNum %d tag %x\n", s.topicName, parNum, tag)
	if s.isEmpty() {
		if err := s.findLastEntryBackward(ctx, protocol.MaxLogSeqnum, parNum); err != nil {
			return nil, err
		}
		if s.isEmpty() {
			return nil, common_errors.ErrStreamEmpty
		}
	}
	seqNumInSharedLog := s.cursor
	// debug.Fprintf(os.Stderr, "cursor: 0x%x, tail: 0x%x\n", s.cursor, s.tail)
	for seqNumInSharedLog < s.tail {
		// debug.Fprintf(os.Stderr, "read tag: 0x%x, seqNum: 0x%x, tail: 0x%x\n",
		// 	tag, seqNumInSharedLog, s.tail)
		newCtx, cancel := context.WithTimeout(ctx, kBlockingReadTimeout)
		defer cancel()
		logEntry, err := s.env.SharedLogReadNextBlock(newCtx, tag, seqNumInSharedLog)
		// debug.Fprintf(os.Stderr, "after read next block\n")
		if err != nil {
			return nil, err
		}
		if logEntry == nil {
			return nil, common_errors.ErrStreamEmpty
		}
		streamLogEntry := decodeStreamLogEntry(logEntry)
		isControl := bits.Has(bits.Bits(streamLogEntry.Meta), Control)
		if streamLogEntry.BelongsToTopic(s.topicName) || isControl {
			isPayloadArr := bits.Has(bits.Bits(streamLogEntry.Meta), PayloadArr)
			s.cursor = logEntry.SeqNum + 1
			return &commtypes.RawMsg{
				Payload:      streamLogEntry.Payload,
				AuxData:      logEntry.AuxData,
				MsgSeqNum:    streamLogEntry.MsgSeqNum,
				LogSeqNum:    logEntry.SeqNum,
				IsControl:    isControl,
				IsPayloadArr: isPayloadArr,
				ProdId: commtypes.ProducerId{
					TaskId:        streamLogEntry.TaskId,
					TaskEpoch:     streamLogEntry.TaskEpoch,
					TransactionID: streamLogEntry.TransactionID,
				},
			}, nil
		}
		seqNumInSharedLog = logEntry.SeqNum + 1
		s.cursor = seqNumInSharedLog
	}
	return nil, common_errors.ErrStreamEmpty
}

func (s *SharedLogStream) readPrevWithTimeout(ctx context.Context, tag uint64, seqNum uint64) (*types.LogEntry, error) {
	maxRetryTimes := 100
	idx := 0
	for {
		newCtx, cancel := context.WithTimeout(ctx, kBlockingReadTimeout)
		defer cancel()
		logEntry, err := s.env.SharedLogReadPrev(newCtx, tag, seqNum)
		if err != nil {
			return nil, err
		}
		if logEntry != nil {
			return logEntry, nil
		} else {
			idx += 1
			if idx >= maxRetryTimes {
				return logEntry, nil
			}
			time.Sleep(100 * time.Microsecond)
		}
	}
}

func (s *SharedLogStream) findLastEntryBackward(ctx context.Context, tailSeqNum uint64, parNum uint8) error {
	if tailSeqNum < s.cursor {
		log.Fatal().
			Uint64("Current seq", s.cursor).
			Uint64("Request seq", tailSeqNum).
			Msg("Cannot sync to request")
		return fmt.Errorf("cannot sync to request")
	}

	/*
		if tailSeqNum == s.cursor+1 {
			return nil
		}
	*/

	tag := NameHashWithPartition(s.topicNameHash, parNum)

	seqNum := tailSeqNum
	// debug.Fprintf(os.Stderr, "find tail for topic: %s, par: %d\n", s.topicName, parNum)
	for seqNum >= s.cursor {
		logEntry, err := s.readPrevWithTimeout(ctx, tag, seqNum)
		if err != nil {
			return err
		}

		if logEntry == nil || logEntry.SeqNum < s.cursor {
			// we are already at the tail
			break
		}
		seqNum = logEntry.SeqNum
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if !streamLogEntry.BelongsToTopic(s.topicName) {
			seqNum -= 1
			continue
		}
		s.mux.Lock()
		s.tail = logEntry.SeqNum + 1
		s.mux.Unlock()
		// debug.Fprintf(os.Stderr, "%s(%d) current tail is %x\n", s.topicName, parNum, s.tail)
		break
	}
	return nil
}
