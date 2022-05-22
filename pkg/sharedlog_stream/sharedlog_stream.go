//go:generate msgp
//msgp:ignore SharedLogStream
package sharedlog_stream

import (
	"context"
	"fmt"
	"math"
	"sharedlog-stream/pkg/bits"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/txn_data"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

const (
	kBlockingReadTimeout = 1 * time.Second
)

const (
	Control bits.Bits = 1 << iota
	PayloadArr
)

type SharedLogStream struct {
	mux sync.Mutex

	env types.Environment
	// txnMarkerSerde commtypes.Serde
	topicName     string
	topicNameHash uint64
	// current read position in forward direction
	cursor             uint64
	tail               uint64
	curAppendMsgSeqNum uint64
}

func NameHashWithPartition(nameHash uint64, par uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<txn_data.PartitionBits - 1)
	return (nameHash & mask) + uint64(par)
}

func TxnMarkerTag(nameHash uint64, par uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(txn_data.PartitionBits+txn_data.LogTagReserveBits) - 1)
	return nameHash&mask + uint64(par)<<txn_data.LogTagReserveBits + txn_data.TxnMarkLowBits
}

type StreamLogEntry struct {
	TopicName     string `msg:"topicName"`
	Payload       []byte `msg:"payload,omitempty"`
	TaskId        uint64 `msg:"tid,omitempty"`
	MsgSeqNum     uint64 `msg:"mseq,omitempty"`
	TransactionID uint64 `msg:"trid,omitempty"`
	TaskEpoch     uint16 `msg:"te,omitempty"`
	Meta          uint8  `msg:"meta,omitempty"`
	seqNum        uint64 `msg:"-"`
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

func NewSharedLogStream(env types.Environment, topicName string, serdeFormat commtypes.SerdeFormat) (*SharedLogStream, error) {
	// var txnMarkerSerde commtypes.Serde
	// if serdeFormat == commtypes.JSON {
	// 	txnMarkerSerde = txn_data.TxnMarkerJSONSerde{}
	// } else if serdeFormat == commtypes.MSGP {
	// 	txnMarkerSerde = txn_data.TxnMarkerMsgpSerde{}
	// } else {
	// 	return nil, fmt.Errorf("unrecognized format: %d", serdeFormat)
	// }
	return &SharedLogStream{
		env:           env,
		topicName:     topicName,
		topicNameHash: NameHash(topicName),
		cursor:        0,
		tail:          0,

		// txnMarkerSerde:     txnMarkerSerde,
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
	s.mux.Lock()
	defer s.mux.Unlock()
	s.cursor = cursor
}

// multiple thread could push to the same stream but only one reader
func (s *SharedLogStream) PushWithTag(ctx context.Context, payload []byte, parNum uint8, tags []uint64,
	isControl bool, payloadIsArr bool, taskId uint64, taskEpoch uint16, transactionID uint64,
) (uint64, error) {
	if len(payload) == 0 {
		return 0, errors.ErrEmptyPayload
	}
	var meta bits.Bits
	if isControl {
		meta = bits.Set(meta, Control)
	}
	if payloadIsArr {
		meta = bits.Set(meta, PayloadArr)
	}
	logEntry := &StreamLogEntry{
		TopicName: s.topicName,
		Payload:   payload,
		Meta:      uint8(meta),
	}
	// if s.inTransaction {
	// TODO: need to deal with sequence number overflow
	atomic.AddUint64(&s.curAppendMsgSeqNum, 1)
	logEntry.MsgSeqNum = s.curAppendMsgSeqNum
	logEntry.TaskEpoch = taskEpoch
	logEntry.TaskId = taskId
	logEntry.TransactionID = transactionID
	// }
	encoded, err := logEntry.MarshalMsg(nil)
	if err != nil {
		return 0, err
	}

	seqNum, err := s.env.SharedLogAppend(ctx, tags, encoded)
	s.mux.Lock()
	s.tail = seqNum + 1
	s.mux.Unlock()

	// verify that push is successful

	if err != nil {
		return 0, err
	}
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

func (s *SharedLogStream) Push(ctx context.Context, payload []byte, parNum uint8, isControl bool, payloadIsArr bool,
	taskId uint64, taskEpoch uint16, transactionID uint64,
) (uint64, error) {
	tags := []uint64{NameHashWithPartition(s.topicNameHash, parNum)}
	return s.PushWithTag(ctx, payload, parNum, tags, isControl, payloadIsArr, taskId, taskEpoch, transactionID)
}

func (s *SharedLogStream) isEmpty() bool {
	s.mux.Lock()
	defer s.mux.Unlock()
	return s.cursor >= s.tail
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
		if streamLogEntry.TopicName != s.topicName {
			continue
		} else {
			isControl := bits.Has(bits.Bits(streamLogEntry.Meta), Control)
			isPayloadArr := bits.Has(bits.Bits(streamLogEntry.Meta), PayloadArr)
			return &commtypes.RawMsg{
				Payload:      streamLogEntry.Payload,
				MsgSeqNum:    streamLogEntry.MsgSeqNum,
				LogSeqNum:    streamLogEntry.seqNum,
				IsControl:    isControl,
				IsPayloadArr: isPayloadArr,
			}, nil
		}
	}
	return nil, errors.ErrStreamEmpty
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
			return nil, errors.ErrStreamEmpty
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
			return nil, errors.ErrStreamEmpty
		}
		streamLogEntry := decodeStreamLogEntry(logEntry)
		isControl := bits.Has(bits.Bits(streamLogEntry.Meta), Control)
		if streamLogEntry.TopicName == s.topicName || bits.Has(bits.Bits(streamLogEntry.Meta), Control) {
			isPayloadArr := bits.Has(bits.Bits(streamLogEntry.Meta), PayloadArr)
			s.cursor = streamLogEntry.seqNum + 1
			return &commtypes.RawMsg{
				Payload:      streamLogEntry.Payload,
				MsgSeqNum:    streamLogEntry.MsgSeqNum,
				LogSeqNum:    streamLogEntry.seqNum,
				IsControl:    isControl,
				IsPayloadArr: isPayloadArr,
				TranId: commtypes.TranIdentifier{
					TaskId:        streamLogEntry.TaskId,
					TaskEpoch:     streamLogEntry.TaskEpoch,
					TransactionID: streamLogEntry.TransactionID,
				},
			}, nil
		}
		seqNumInSharedLog = logEntry.SeqNum + 1
		s.cursor = seqNumInSharedLog
	}
	return nil, errors.ErrStreamEmpty
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
			time.Sleep(10 * time.Millisecond)
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
		// debug.Fprintf(os.Stderr, "current sequence number: 0x%x, tail: 0x%x, tag: %x\n", seqNum, s.tail, tag)
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
		if streamLogEntry.TopicName != s.topicName {
			seqNum -= 1
			continue
		}
		s.tail = logEntry.SeqNum + 1
		// debug.Fprintf(os.Stderr, "current tail is %d\n", s.tail)
		break
	}
	return nil
}
