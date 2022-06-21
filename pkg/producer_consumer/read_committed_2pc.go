package producer_consumer

import (
	"context"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"

	"github.com/gammazero/deque"
)

type TransactionAwareConsumer struct {
	epochMarkerSerde commtypes.Serde
	stream           *sharedlog_stream.ShardedSharedLogStream
	committed        map[commtypes.ProducerId]struct{}
	aborted          map[commtypes.ProducerId]struct{}
	// check whether producer produces duplicate records
	curReadMsgSeqNum map[commtypes.ProducerId]uint64
	msgBuffer        []*deque.Deque
}

func NewTransactionAwareConsumer(stream *sharedlog_stream.ShardedSharedLogStream,
	serdeFormat commtypes.SerdeFormat,
) (*TransactionAwareConsumer, error) {
	epochMarkerSerde, err := commtypes.GetEpochMarkerSerde(serdeFormat)
	if err != nil {
		return nil, err
	}
	return &TransactionAwareConsumer{
		msgBuffer:        make([]*deque.Deque, stream.NumPartition()),
		stream:           stream,
		epochMarkerSerde: epochMarkerSerde,
		committed:        make(map[commtypes.ProducerId]struct{}),
		aborted:          make(map[commtypes.ProducerId]struct{}),
		curReadMsgSeqNum: make(map[commtypes.ProducerId]uint64),
	}, nil
}

func (tac *TransactionAwareConsumer) checkMsgQueue(msgQueue *deque.Deque, parNum uint8) *commtypes.RawMsg {
	if msgQueue.Len() > 0 {
		frontMsg := msgQueue.Front().(*commtypes.RawMsg)
		if frontMsg.IsControl && frontMsg.Mark == commtypes.EPOCH_END {
			delete(tac.committed, frontMsg.ProdId)
			debug.Fprintf(os.Stderr, "remove commit mark 0x%x\n", frontMsg.LogSeqNum)
			msgQueue.PopFront()
			if msgQueue.Len() > 0 {
				frontMsg = msgQueue.Front().(*commtypes.RawMsg)
			} else {
				return nil
			}
		} else if frontMsg.IsControl && frontMsg.Mark == commtypes.ABORT {
			delete(tac.committed, frontMsg.ProdId)
			msgQueue.PopFront()
			if msgQueue.Len() > 0 {
				frontMsg = msgQueue.Front().(*commtypes.RawMsg)
			} else {
				return nil
			}
		}
		if frontMsg.ScaleEpoch != 0 {
			msgQueue.PopFront()
			return frontMsg
		}
		if tac.HasCommited(frontMsg.ProdId) {
			msgQueue.PopFront()
			debug.Fprintf(os.Stderr, "return msg 0x%x\n", frontMsg.LogSeqNum)
			return frontMsg
		}
		for tac.HasAborted(frontMsg.ProdId) {
			msgQueue.PopFront()
			frontMsg = msgQueue.Front().(*commtypes.RawMsg)
		}
	}
	return nil
}

func (tac *TransactionAwareConsumer) ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	if tac.msgBuffer[parNum] == nil {
		tac.msgBuffer[parNum] = deque.New()
	}

	msgQueue := tac.msgBuffer[parNum]
	debug.Fprintf(os.Stderr, "reading from sub %d, msg queue len: %d\n", parNum, msgQueue.Len())
	if msgQueue.Len() != 0 {
		retMsg := tac.checkMsgQueue(msgQueue, parNum)
		if retMsg != nil {
			debug.Fprintf(os.Stderr, "return msg in check 1\n")
			return retMsg, nil
		}
	}
	for {
		rawMsg, err := tac.stream.ReadNext(ctx, parNum)
		if err != nil {
			debug.Fprintf(os.Stderr, "[ERROR] return err: %v\n", err)
			return nil, err
		}
		// debug.Fprintf(os.Stderr, "RawMsg\n")
		// debug.Fprintf(os.Stderr, "\tPayload %v\n", string(rawMsg.Payload))
		// debug.Fprintf(os.Stderr, "\tLogSeq 0x%x\n", rawMsg.LogSeqNum)
		// debug.Fprintf(os.Stderr, "\tProdId taskId 0x%x, taskEpoch %d, tranID %d\n",
		// 	rawMsg.ProdId.TaskId, rawMsg.ProdId.TaskEpoch, rawMsg.ProdId.TransactionID)
		// debug.Fprintf(os.Stderr, "\tIsControl: %v\n", rawMsg.IsControl)
		// debug.Fprintf(os.Stderr, "\tIsPayloadArr: %v\n", rawMsg.IsPayloadArr)
		if shouldIgnoreThisMsg(tac.curReadMsgSeqNum, rawMsg) {
			debug.Fprintf(os.Stderr, "got a duplicate entry; continue\n")
			continue
		}
		if rawMsg.IsControl {
			txnMarkTmp, err := tac.epochMarkerSerde.Decode(rawMsg.Payload)
			if err != nil {
				debug.Fprintf(os.Stderr, "[ERROR] return err2: %v\n", err)
				return nil, err
			}
			txnMark := txnMarkTmp.(commtypes.EpochMarker)
			if txnMark.Mark == commtypes.EPOCH_END {
				debug.Fprintf(os.Stderr, "Got commit msg with tranid: %v\n", rawMsg.ProdId)
				tac.committed[rawMsg.ProdId] = struct{}{}
				rawMsg.Mark = commtypes.EPOCH_END
			} else if txnMark.Mark == commtypes.ABORT {
				// debug.Fprintf(os.Stderr, "Got abort msg with tranid: %v\n", rawMsg.TranId)
				tac.aborted[rawMsg.ProdId] = struct{}{}
				rawMsg.Mark = commtypes.ABORT
			} else if txnMark.Mark == commtypes.SCALE_FENCE {
				rawMsg.ScaleEpoch = txnMark.ScaleEpoch
			}
		}
		msgQueue.PushBack(rawMsg)
		retMsg := tac.checkMsgQueue(msgQueue, parNum)
		if retMsg != nil {
			debug.Fprintf(os.Stderr, "return msg in check 2\n")
			return retMsg, nil
		}
	}
}

func (tac *TransactionAwareConsumer) HasCommited(tranId commtypes.ProducerId) bool {
	// debug.Fprintf(os.Stderr, "committed has %v, tranId: %v\n", tac.committed, tranId)
	_, ok := tac.committed[tranId]
	return ok
}

func (tas *TransactionAwareConsumer) HasAborted(tranId commtypes.ProducerId) bool {
	_, ok := tas.aborted[tranId]
	return ok
}
