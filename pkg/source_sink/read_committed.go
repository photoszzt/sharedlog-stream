package source_sink

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/txn_data"

	"github.com/gammazero/deque"
)

type TransactionAwareConsumer struct {
	txnMarkerSerde   commtypes.Serde
	stream           *sharedlog_stream.ShardedSharedLogStream
	committed        map[commtypes.TranIdentifier]struct{}
	aborted          map[commtypes.TranIdentifier]struct{}
	curReadMsgSeqNum map[commtypes.TranIdentifier]uint64
	msgBuffer        []*deque.Deque
}

func NewTransactionAwareConsumer(stream *sharedlog_stream.ShardedSharedLogStream, serdeFormat commtypes.SerdeFormat) (*TransactionAwareConsumer, error) {
	var txnMarkerSerde commtypes.Serde
	if serdeFormat == commtypes.JSON {
		txnMarkerSerde = txn_data.TxnMarkerJSONSerde{}
	} else if serdeFormat == commtypes.MSGP {
		txnMarkerSerde = txn_data.TxnMarkerMsgpSerde{}
	} else {
		return nil, fmt.Errorf("unrecognized format: %d", serdeFormat)
	}
	return &TransactionAwareConsumer{
		msgBuffer:        make([]*deque.Deque, stream.NumPartition()),
		stream:           stream,
		txnMarkerSerde:   txnMarkerSerde,
		committed:        make(map[commtypes.TranIdentifier]struct{}),
		aborted:          make(map[commtypes.TranIdentifier]struct{}),
		curReadMsgSeqNum: make(map[commtypes.TranIdentifier]uint64),
	}, nil
}

func (tac *TransactionAwareConsumer) checkCommitted(msgQueue *deque.Deque) *commtypes.RawMsg {
	if msgQueue.Len() > 0 {
		frontMsg := msgQueue.Front().(*commtypes.RawMsg)
		if frontMsg.ScaleEpoch != 0 {
			msgQueue.PopFront()
			return frontMsg
		}
		if tac.HasCommited(frontMsg.TranId) {
			msgQueue.PopFront()
			return frontMsg
		}
	}
	return nil
}

func (tac *TransactionAwareConsumer) dropAborted(msgQueue *deque.Deque) {
	if msgQueue.Len() > 0 {
		frontMsg := msgQueue.Front().(*commtypes.RawMsg)
		for tac.HasAborted(frontMsg.TranId) {
			msgQueue.PopFront()
			frontMsg = msgQueue.Front().(*commtypes.RawMsg)
		}
	}
}

func (tac *TransactionAwareConsumer) checkControlMsg(msgQueue *deque.Deque) {
	if msgQueue.Len() > 0 {
		frontMsg := msgQueue.Front().(*commtypes.RawMsg)
		if frontMsg.IsControl && frontMsg.Mark == txn_data.COMMIT {
			delete(tac.committed, frontMsg.TranId)
			msgQueue.PopFront()
		} else if frontMsg.IsControl && frontMsg.Mark == txn_data.ABORT {
			delete(tac.committed, frontMsg.TranId)
			msgQueue.PopFront()
		}
	}
}

func (tac *TransactionAwareConsumer) ReadNext(ctx context.Context, parNum uint8) (*commtypes.RawMsg, error) {
	if tac.msgBuffer[parNum] == nil {
		tac.msgBuffer[parNum] = deque.New()
	}
	msgQueue := tac.msgBuffer[parNum]
	if msgQueue.Len() != 0 && len(tac.committed) != 0 {
		tac.checkControlMsg(msgQueue)
		ret := tac.checkCommitted(msgQueue)
		if ret != nil {
			debug.Fprintf(os.Stderr, "return output1 %v\n", string(ret.Payload))
			return ret, nil
		}
		tac.dropAborted(msgQueue)
	}
	for {
		rawMsg, err := tac.stream.ReadNext(ctx, parNum)
		if err != nil {
			return nil, err
		}
		// debug.Fprintf(os.Stderr, "RawMsg\n")
		// debug.Fprintf(os.Stderr, "\tPayload %v\n", string(rawMsg.Payload))
		// debug.Fprintf(os.Stderr, "\tLogSeq 0x%x\n", rawMsg.LogSeqNum)
		// debug.Fprintf(os.Stderr, "\tIsControl: %v\n", rawMsg.IsControl)
		// debug.Fprintf(os.Stderr, "\tIsPayloadArr: %v\n", rawMsg.IsPayloadArr)
		tranId := rawMsg.TranId
		msgSeqNum, ok := tac.curReadMsgSeqNum[tranId]
		if ok && msgSeqNum == rawMsg.MsgSeqNum {
			debug.Fprintf(os.Stderr, "got a duplicate entry; continue\n")
			continue
		}

		tac.curReadMsgSeqNum[tranId] = rawMsg.MsgSeqNum
		if rawMsg.IsControl {
			txnMarkTmp, err := tac.txnMarkerSerde.Decode(rawMsg.Payload)
			if err != nil {
				return nil, err
			}
			txnMark := txnMarkTmp.(txn_data.TxnMarker)
			if txnMark.Mark == uint8(txn_data.COMMIT) {
				debug.Fprintf(os.Stderr, "Got commit msg with tranid: %v\n", rawMsg.TranId)
				tac.committed[rawMsg.TranId] = struct{}{}
				rawMsg.Mark = txn_data.COMMIT
			} else if txnMark.Mark == uint8(txn_data.ABORT) {
				// debug.Fprintf(os.Stderr, "Got abort msg with tranid: %v\n", rawMsg.TranId)
				tac.aborted[rawMsg.TranId] = struct{}{}
				rawMsg.Mark = txn_data.ABORT
			} else if txnMark.Mark == uint8(txn_data.SCALE_FENCE) {
				rawMsg.ScaleEpoch = txnMark.TranIDOrScaleEpoch
			}
		}
		msgQueue.PushBack(rawMsg)
		tac.checkControlMsg(msgQueue)
		ret := tac.checkCommitted(msgQueue)
		if ret != nil {
			return ret, nil
		}
		tac.dropAborted(msgQueue)
	}
}

func (tac *TransactionAwareConsumer) HasCommited(tranId commtypes.TranIdentifier) bool {
	// debug.Fprintf(os.Stderr, "committed has %v, tranId: %v\n", tac.committed, tranId)
	_, ok := tac.committed[tranId]
	return ok
}

func (tas *TransactionAwareConsumer) HasAborted(tranId commtypes.TranIdentifier) bool {
	_, ok := tas.aborted[tranId]
	return ok
}
