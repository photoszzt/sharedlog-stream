package commtypes

const (
	SCALE_FENCE_KEY = "__scale_fence"
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

type RawMsg struct {
	Payload      []byte
	IsControl    bool
	IsPayloadArr bool
	MsgSeqNum    uint64
	LogSeqNum    uint64
	ScaleEpoch   uint64
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
