package commtypes

import (
	"fmt"
	"sharedlog-stream/pkg/txn_data"
)

type Message struct {
	Key       interface{}
	Value     interface{}
	Timestamp int64
}

var EmptyMessage = Message{}

type TaskIDGen struct {
	TaskId    uint64
	TaskEpoch uint16
}

var EmptyAppIDGen = TaskIDGen{TaskId: 0, TaskEpoch: 0}
var EmptyRawMsg = RawMsg{Payload: nil, MsgSeqNum: 0, LogSeqNum: 0}

type ReadMsgAndProgress struct {
	MsgBuff          []RawMsg
	CurReadMsgSeqNum uint64
}

type TranIdentifier struct {
	TaskId        uint64
	TransactionID uint64
	TaskEpoch     uint16
}

type RawMsg struct {
	Payload []byte

	MsgSeqNum  uint64
	LogSeqNum  uint64
	ScaleEpoch uint64

	TranId TranIdentifier

	IsControl    bool
	IsPayloadArr bool
	Mark         txn_data.TxnMark
}

type MsgAndSeq struct {
	Msg       Message
	MsgArr    []Message
	MsgSeqNum uint64
	LogSeqNum uint64
	IsControl bool
}

type MsgAndSeqs struct {
	Msgs     []MsgAndSeq
	TotalLen uint32
}

func DecodeRawMsg(rawMsg *RawMsg, serdes interface{},
	payloadArrSerde Serde,
	decodeMsgFunc func(payload []byte, serdes interface{}) (Message, error),
) (*MsgAndSeq, error) {
	if rawMsg.IsPayloadArr {
		payloadArrTmp, err := payloadArrSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, err
		}
		payloadArr := payloadArrTmp.(PayloadArr)
		var msgArr []Message
		for _, payload := range payloadArr.Payloads {
			msg, err := decodeMsgFunc(payload, serdes)
			if err != nil {
				return nil, err
			}
			msgArr = append(msgArr, msg)
		}
		return &MsgAndSeq{MsgArr: msgArr, Msg: EmptyMessage,
			MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum}, nil
	} else {
		msg, err := decodeMsgFunc(rawMsg.Payload, serdes)
		if err != nil {
			return nil, err
		}
		return &MsgAndSeq{
			Msg:       msg,
			MsgArr:    nil,
			MsgSeqNum: rawMsg.MsgSeqNum,
			LogSeqNum: rawMsg.LogSeqNum,
			IsControl: false,
		}, nil
	}
}

func DecodeMsg(payload []byte,
	kvmsgSerdesTmp interface{},
) (Message, error) {
	kvmsgSerdes := kvmsgSerdesTmp.(KVMsgSerdes)
	keyEncoded, valueEncoded, err := kvmsgSerdes.MsgSerde.Decode(payload)
	if err != nil {
		return EmptyMessage, fmt.Errorf("fail to decode msg: %v", err)
	}
	var key interface{} = nil
	if keyEncoded != nil {
		key, err = kvmsgSerdes.KeySerde.Decode(keyEncoded)
		if err != nil {
			return EmptyMessage, fmt.Errorf("fail to decode key: %v", err)
		}
	}
	var value interface{} = nil
	if valueEncoded != nil {
		value, err = kvmsgSerdes.ValSerde.Decode(valueEncoded)
		if err != nil {
			return EmptyMessage, fmt.Errorf("fail to decode value: %v", err)
		}
	}
	if key == nil && value == nil {
		return EmptyMessage, nil
	}
	return Message{Key: key, Value: value}, nil
}

func EncodeMsg(msg Message, kvmsgSerdes KVMsgSerdes) ([]byte, error) {
	var keyEncoded []byte
	if msg.Key != nil {
		keyEncodedTmp, err := kvmsgSerdes.KeySerde.Encode(msg.Key)
		if err != nil {
			return nil, err
		}
		keyEncoded = keyEncodedTmp
	}
	var valEncoded []byte
	if msg.Value != nil {
		valEncodedTmp, err := kvmsgSerdes.ValSerde.Encode(msg.Value)
		if err != nil {
			return nil, err
		}
		valEncoded = valEncodedTmp
	}
	if keyEncoded == nil && valEncoded == nil {
		return nil, nil
	}
	bytes, err := kvmsgSerdes.MsgSerde.Encode(keyEncoded, valEncoded)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}
