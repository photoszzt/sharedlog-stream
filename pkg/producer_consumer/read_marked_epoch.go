package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"

	"github.com/gammazero/deque"
)

type MsgStatus uint8

const (
	NOT_MARK MsgStatus = iota
	MARKED
	SHOULD_DROP
)

type EpochMarkConsumer struct {
	epochMarkerSerde commtypes.SerdeG[commtypes.EpochMarker]
	stream           *sharedlog_stream.ShardedSharedLogStream
	marked           map[commtypes.ProducerId]map[uint8]commtypes.ProduceRange
	curReadMsgSeqNum map[commtypes.ProducerId]uint64
	msgBuffer        []*deque.Deque
}

func NewEpochMarkConsumer(stream *sharedlog_stream.ShardedSharedLogStream,
	serdeFormat commtypes.SerdeFormat,
) (*EpochMarkConsumer, error) {
	epochMarkSerde, err := commtypes.GetEpochMarkerSerdeG(serdeFormat)
	if err != nil {
		return nil, err
	}
	return &EpochMarkConsumer{
		epochMarkerSerde: epochMarkSerde,
		stream:           stream,
		marked:           make(map[commtypes.ProducerId]map[uint8]commtypes.ProduceRange),
		msgBuffer:        make([]*deque.Deque, stream.NumPartition()),
		curReadMsgSeqNum: make(map[commtypes.ProducerId]uint64),
	}, nil
}

func (emc *EpochMarkConsumer) ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	if emc.msgBuffer[parNum] == nil {
		emc.msgBuffer[parNum] = deque.New()
	}
	msgQueue := emc.msgBuffer[parNum]
	if msgQueue.Len() != 0 {
		retMsg := emc.checkMsgQueue(msgQueue, parNum)
		if retMsg != nil {
			return retMsg, nil
		}
	}
	for {
		rawMsg, err := emc.stream.ReadNext(ctx, parNum)
		if err != nil {
			return nil, err
		}
		// debug.Fprintf(os.Stderr, "RawMsg\n")
		// debug.Fprintf(os.Stderr, "\tPayload %v\n", string(rawMsg.Payload))
		// debug.Fprintf(os.Stderr, "\tLogSeq 0x%x\n", rawMsg.LogSeqNum)
		// debug.Fprintf(os.Stderr, "\tIsControl: %v\n", rawMsg.IsControl)
		// debug.Fprintf(os.Stderr, "\tIsPayloadArr: %v\n", rawMsg.IsPayloadArr)

		// if !rawMsg.IsControl {
		// 	debug.Fprintf(os.Stderr, "%s RawMsg: Payload %v, LogSeq 0x%x, MsgSeqNum 0x%x, IsControl: %v, IsPayloadArr: %v\n",
		// 		emc.stream.TopicName(), string(rawMsg.Payload), rawMsg.LogSeqNum, rawMsg.MsgSeqNum, rawMsg.IsControl, rawMsg.IsPayloadArr)
		// }

		// control entry's msgseqnum is written for the epoch log;
		// reading from the normal stream, we don't count this entry as a duplicate one
		// control entry itself is idempodent; n commit record with the same content is the
		// same as one commit record
		if !rawMsg.IsControl {
			if shouldIgnoreThisMsg(emc.curReadMsgSeqNum, rawMsg) {
				fmt.Fprintf(os.Stderr, "got a duplicate entry; continue\n")
				continue
			}
		}
		if rawMsg.IsControl {
			epochMark, err := emc.epochMarkerSerde.Decode(rawMsg.Payload)
			if err != nil {
				return nil, err
			}
			// debug.Fprintf(os.Stderr, "%+v\n", epochMark)
			if epochMark.Mark == commtypes.EPOCH_END {
				// debug.Fprintf(os.Stderr, "%+v\n", epochMark)
				ranges, ok := emc.marked[rawMsg.ProdId]
				if !ok {
					ranges = make(map[uint8]commtypes.ProduceRange)
				}
				markRanges := epochMark.OutputRanges[emc.stream.TopicName()]
				for _, r := range markRanges {
					ranges[r.SubStreamNum] = r
				}
				emc.marked[rawMsg.ProdId] = ranges
				rawMsg.Mark = commtypes.EPOCH_END
			} else if epochMark.Mark == commtypes.SCALE_FENCE {
				rawMsg.ScaleEpoch = epochMark.ScaleEpoch
				rawMsg.Mark = epochMark.Mark
			} else if epochMark.Mark == commtypes.STREAM_END {
				rawMsg.Mark = epochMark.Mark
				rawMsg.StartTime = epochMark.StartTime
				rawMsg.ProdIdx = epochMark.ProdIndex
			}
		}

		msgQueue.PushBack(rawMsg)
		retMsg := emc.checkMsgQueue(msgQueue, parNum)
		if retMsg != nil {
			return retMsg, nil
		}
	}
}

func (emc *EpochMarkConsumer) checkMsgQueue(msgQueue *deque.Deque, parNum uint8) *commtypes.RawMsg {
	if msgQueue.Len() > 0 {
		frontMsg := msgQueue.Front().(*commtypes.RawMsg)
		for frontMsg.IsControl && frontMsg.Mark == commtypes.EPOCH_END {
			ranges := emc.marked[frontMsg.ProdId]
			produce := commtypes.ProduceRange{}
			produce.Start = 0
			produce.End = 0
			produce.SubStreamNum = parNum
			ranges[parNum] = produce
			msgQueue.PopFront()
			if msgQueue.Len() > 0 {
				frontMsg = msgQueue.Front().(*commtypes.RawMsg)
			} else {
				return nil
			}
		}
		if frontMsg.Mark == commtypes.SCALE_FENCE && frontMsg.ScaleEpoch != 0 {
			frontMsg := msgQueue.PopFront().(*commtypes.RawMsg)
			return frontMsg
		}
		if frontMsg.Mark == commtypes.STREAM_END {
			frontMsg := msgQueue.PopFront().(*commtypes.RawMsg)
			return frontMsg
		}
		msgStatus := emc.checkMsgStatus(frontMsg, parNum)
		if msgStatus == MARKED {
			msgQueue.PopFront()
			return frontMsg
		}
		for msgStatus == SHOULD_DROP {
			msgQueue.PopFront()
			if msgQueue.Len() > 0 {
				frontMsg = msgQueue.Front().(*commtypes.RawMsg)
				msgStatus = emc.checkMsgStatus(frontMsg, parNum)
			} else {
				return nil
			}
		}
	}
	return nil
}

func (emc *EpochMarkConsumer) checkMsgStatus(rawMsg *commtypes.RawMsg, parNum uint8) MsgStatus {
	ranges, ok := emc.marked[rawMsg.ProdId]
	if !ok {
		return NOT_MARK
	}
	markedRange := ranges[parNum]
	if markedRange.Start == 0 && markedRange.End == 0 {
		return NOT_MARK
	}
	if rawMsg.LogSeqNum < markedRange.Start {
		return SHOULD_DROP
	} else if rawMsg.LogSeqNum >= markedRange.Start && rawMsg.LogSeqNum <= markedRange.End {
		return MARKED
	} else {
		// the entry is after this marker;
		return NOT_MARK
	}
}

func shouldIgnoreThisMsg(curReadMsgSeqNum map[commtypes.ProducerId]uint64, rawMsg *commtypes.RawMsg) bool {
	prodId := rawMsg.ProdId
	msgSeqNum, ok := curReadMsgSeqNum[prodId]
	if ok && msgSeqNum == rawMsg.MsgSeqNum {

		return true
	}

	curReadMsgSeqNum[prodId] = rawMsg.MsgSeqNum
	return false
}
