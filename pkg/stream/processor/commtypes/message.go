package commtypes

type Message struct {
	Key       interface{}
	Value     interface{}
	Timestamp uint64
}

var EmptyMessage = Message{}

type AppIDGen struct {
	AppId    uint64
	AppEpoch uint16
}

var EmptyAppIDGen = AppIDGen{AppId: 0, AppEpoch: 0}
var EmptyRawMsg = RawMsg{Payload: nil, MsgSeqNum: 0, LogSeqNum: 0}

type ReadMsgAndProgress struct {
	MsgBuff          []RawMsg
	CurReadMsgSeqNum uint32
}

type RawMsg struct {
	Payload   []byte
	MsgSeqNum uint32
	LogSeqNum uint64
}

type MsgAndSeq struct {
	Msg       Message
	MsgSeqNum uint32
	LogSeqNum uint64
}
