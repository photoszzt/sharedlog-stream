package producer_consumer

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stats"
	"time"

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
	marked           map[commtypes.ProducerId]map[uint8]commtypes.ProduceRangeWithEnd
	curReadMsgSeqNum map[commtypes.ProducerId]uint64
	msgBuffer        []*deque.Deque
	streamTime       stats.PrintLogStatsCollector[int64]
}

func NewEpochMarkConsumer(
	srcName string,
	stream *sharedlog_stream.ShardedSharedLogStream,
	serdeFormat commtypes.SerdeFormat,
) (*EpochMarkConsumer, error) {
	epochMarkSerde, err := commtypes.GetEpochMarkerSerdeG(serdeFormat)
	if err != nil {
		return nil, err
	}
	return &EpochMarkConsumer{
		epochMarkerSerde: epochMarkSerde,
		stream:           stream,
		marked:           make(map[commtypes.ProducerId]map[uint8]commtypes.ProduceRangeWithEnd),
		msgBuffer:        make([]*deque.Deque, stream.NumPartition()),
		curReadMsgSeqNum: make(map[commtypes.ProducerId]uint64),
		streamTime:       stats.NewPrintLogStatsCollector[int64]("streamTime" + srcName),
	}, nil
}

func (emc *EpochMarkConsumer) OutputRemainingStats() {
	emc.streamTime.PrintRemainingStats()
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
		nowMs := time.Now().UnixMilli()
		emc.streamTime.AddSample(nowMs - rawMsg.InjTsMs)
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
		} else {
			epochMark, err := emc.epochMarkerSerde.Decode(rawMsg.Payload)
			if err != nil {
				return nil, err
			}
			// debug.Fprintf(os.Stderr, "%+v\n", epochMark)
			if epochMark.Mark == commtypes.EPOCH_END {
				// debug.Fprintf(os.Stderr, "%+v\n", epochMark)
				ranges, ok := emc.marked[rawMsg.ProdId]
				if !ok {
					ranges = make(map[uint8]commtypes.ProduceRangeWithEnd)
				}
				markRanges := epochMark.OutputRanges[emc.stream.TopicName()]
				for _, r := range markRanges {
					ranges[r.SubStreamNum] = commtypes.ProduceRangeWithEnd{
						ProduceRange: r,
						End:          rawMsg.LogSeqNum,
					}
				}
				emc.marked[rawMsg.ProdId] = ranges
				rawMsg.Mark = commtypes.EPOCH_END
			} else if epochMark.Mark == commtypes.SCALE_FENCE {
				rawMsg.ScaleEpoch = epochMark.ScaleEpoch
				rawMsg.Mark = epochMark.Mark
				rawMsg.ProdIdx = epochMark.ProdIndex
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

func (emc *EpochMarkConsumer) checkMsg(msgQueue *deque.Deque, parNum uint8, frontMsg *commtypes.RawMsg) *commtypes.RawMsg {
	if frontMsg.IsControl && frontMsg.Mark == commtypes.EPOCH_END {
		ranges := emc.marked[frontMsg.ProdId]
		produce := commtypes.ProduceRangeWithEnd{ProduceRange: commtypes.ProduceRange{Start: 0, SubStreamNum: parNum}, End: 0}
		ranges[parNum] = produce
		msgQueue.PopFront()
		return frontMsg
	}
	if (frontMsg.Mark == commtypes.SCALE_FENCE && frontMsg.ScaleEpoch != 0) || frontMsg.Mark == commtypes.STREAM_END {
		msgQueue.PopFront()
		return frontMsg
	}
	return nil
}

func (emc *EpochMarkConsumer) checkMsgQueue(msgQueue *deque.Deque, parNum uint8) *commtypes.RawMsg {
	if msgQueue.Len() > 0 {
		frontMsg := msgQueue.Front().(*commtypes.RawMsg)
		fmt.Fprintf(os.Stderr, "frontMsg: %+v\n", frontMsg)
		readyMsg := emc.checkMsg(msgQueue, parNum, frontMsg)
		if readyMsg != nil {
			return readyMsg
		}
		msgStatus := emc.checkMsgStatus(frontMsg, parNum)
		if msgStatus == MARKED {
			msgQueue.PopFront()
			return frontMsg
		}
		for msgStatus == SHOULD_DROP {
			frontMsg = msgQueue.PopFront().(*commtypes.RawMsg)
			fmt.Fprintf(os.Stderr, "drop msg: %+v\n", frontMsg)
			if msgQueue.Len() > 0 {
				frontMsg = msgQueue.Front().(*commtypes.RawMsg)
				readyMsg := emc.checkMsg(msgQueue, parNum, frontMsg)
				if readyMsg != nil {
					return readyMsg
				}
				msgStatus = emc.checkMsgStatus(frontMsg, parNum)
				if msgStatus == MARKED {
					msgQueue.PopFront()
					return frontMsg
				}
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
		fmt.Fprintf(os.Stderr, "no ranges for prodid %+v\n", rawMsg.ProdId)
		return NOT_MARK
	}
	markedRange := ranges[parNum]
	if markedRange.Start == 0 && markedRange.End == 0 {
		fmt.Fprintf(os.Stderr, "no marked range for prodid %+v, parnum %d\n", rawMsg.ProdId, parNum)
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
