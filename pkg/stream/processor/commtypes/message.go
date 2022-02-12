package commtypes

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

type RawMsg struct {
	Payload   []byte
	IsControl bool
	MsgSeqNum uint64
	LogSeqNum uint64
}

type MsgAndSeq struct {
	Msg       Message
	MsgSeqNum uint64
	LogSeqNum uint64
}
