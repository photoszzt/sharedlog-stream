//go:generate msgp
//msgp:ignore SharedLogStream
package sharedlog_stream

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"

	"github.com/rs/zerolog/log"

	"cs.utexas.edu/zjia/faas/protocol"
	"cs.utexas.edu/zjia/faas/types"
)

type SharedLogStream struct {
	ctx                context.Context
	env                types.Environment
	txnMarkerSerde     commtypes.Serde
	curReadMap         map[commtypes.AppIDGen]commtypes.ReadMsgAndProgress
	topicName          string
	topicNameHash      uint64
	appId              uint64
	cursor             uint64
	tail               uint64
	curAppendMsgSeqNum uint32
	appEpoch           uint16
	inTransaction      bool
}

const (
	PartitionBits = 8
)

func NameHashWithPartition(nameHash uint64, par uint8) uint64 {
	return (nameHash << PartitionBits) + uint64(par)
}

type StreamLogEntry struct {
	TopicName string `msg:"topicName"`
	Payload   []byte `msg:"payload,omitempty"`
	AppId     uint64 `msg:"aid,omitempty"`
	MsgSeqNum uint32 `msg:"mseq,omitempty"`
	AppEpoch  uint16 `msg:"ae,omitempty"`
	IsControl bool   `msg:"isCtrl"`
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

		appId:              0,
		appEpoch:           0,
		curAppendMsgSeqNum: 0,
	}
}

func (s *SharedLogStream) SetAppendMsgSeqNum(val uint32) {
	s.curAppendMsgSeqNum = val
}

func (s *SharedLogStream) SetAppId(appId uint64) {
	s.appId = appId
}

func (s *SharedLogStream) SetAppEpoch(epoch uint16) {
	s.appEpoch = epoch
}

func (s *SharedLogStream) TopicNameHash() uint64 {
	return s.topicNameHash
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

func (s *SharedLogStream) Push(ctx context.Context, payload []byte, parNum uint8, isControl bool) (uint64, error) {
	if len(payload) == 0 {
		return 0, errEmptyPayload
	}
	logEntry := &StreamLogEntry{
		TopicName: s.topicName,
		Payload:   payload,
		IsControl: isControl,
	}
	if s.inTransaction {
		// TODO: need to deal with sequence number overflow
		s.curAppendMsgSeqNum += 1
		logEntry.MsgSeqNum = s.curAppendMsgSeqNum
		logEntry.AppEpoch = s.appEpoch
		logEntry.AppId = s.appId
	}
	encoded, err := logEntry.MarshalMsg(nil)
	if err != nil {
		return 0, err
	}
	tags := []uint64{NameHashWithPartition(s.topicNameHash, parNum)}
	seqNum, err := s.env.SharedLogAppend(ctx, tags, encoded)
	s.tail = seqNum
	return seqNum, err
}

func (s *SharedLogStream) isEmpty() bool {
	return s.cursor >= s.tail
}

func (s *SharedLogStream) ReadNext(ctx context.Context, parNum uint8) (commtypes.AppIDGen, []commtypes.RawMsg, error) {
	if s.isEmpty() {
		if err := s.findLastEntryBackward(ctx, protocol.MaxLogSeqnum); err != nil {
			return commtypes.EmptyAppIDGen, nil, err
		}
		if s.isEmpty() {
			return commtypes.EmptyAppIDGen, nil, errStreamEmpty
		}
	}
	seqNumInSharedLog := s.cursor
	tag := NameHashWithPartition(s.topicNameHash, parNum)
	for seqNumInSharedLog < s.tail {
		logEntry, err := s.env.SharedLogReadNextBlock(ctx, tag, seqNumInSharedLog)
		if err != nil {
			return commtypes.EmptyAppIDGen, nil, err
		}
		if logEntry == nil || logEntry.SeqNum >= s.tail {
			return commtypes.EmptyAppIDGen, nil, errStreamEmpty
		}
		streamLogEntry := decodeStreamLogEntry(logEntry)
		if streamLogEntry.TopicName == s.topicName {
			if s.inTransaction {
				appKey := commtypes.AppIDGen{
					AppId:    streamLogEntry.AppId,
					AppEpoch: streamLogEntry.AppEpoch,
				}
				readMsgProc, ok := s.curReadMap[appKey]
				if streamLogEntry.IsControl {
					txnMarkTmp, err := s.txnMarkerSerde.Decode(streamLogEntry.Payload)
					if err != nil {
						return commtypes.EmptyAppIDGen, nil, err
					}
					txnMark := txnMarkTmp.(TxnMarker)
					if txnMark.Mark == uint8(COMMIT) {
						if !ok {
							log.Warn().Msgf("Hit commit marker but got no messages")
						}
						msgBuf := readMsgProc.MsgBuff
						delete(s.curReadMap, appKey)
						s.cursor = streamLogEntry.seqNum + 1
						return appKey, msgBuf, nil
					} else if txnMark.Mark == uint8(ABORT) {
						// abort, drop the current buffered msgs
						delete(s.curReadMap, appKey)
						seqNumInSharedLog = logEntry.SeqNum + 1
						continue
					}
				}
				if !ok {
					readMsgProc = commtypes.ReadMsgAndProgress{
						CurReadMsgSeqNum: 0,
						MsgBuff:          make([]commtypes.RawMsg, 0),
					}
				}
				if streamLogEntry.MsgSeqNum == readMsgProc.CurReadMsgSeqNum {
					// encounter duplicate value, ignore it
					seqNumInSharedLog = logEntry.SeqNum + 1
					continue
				}
				readMsgProc.CurReadMsgSeqNum = uint32(streamLogEntry.seqNum)
				readMsgProc.MsgBuff = append(readMsgProc.MsgBuff, commtypes.RawMsg{})
				s.curReadMap[appKey] = readMsgProc
				seqNumInSharedLog = logEntry.SeqNum + 1
				continue
			}
			s.cursor = streamLogEntry.seqNum + 1
			return commtypes.EmptyAppIDGen, []commtypes.RawMsg{{Payload: streamLogEntry.Payload, MsgSeqNum: 0, LogSeqNum: streamLogEntry.seqNum}}, nil
		}
		seqNumInSharedLog = logEntry.SeqNum + 1
	}
	return commtypes.EmptyAppIDGen, nil, errStreamEmpty
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
