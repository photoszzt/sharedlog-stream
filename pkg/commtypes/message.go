package commtypes

type Message struct {
	Key       interface{}
	Value     interface{}
	Timestamp int64
	InjT      int64 `msg:"injT" json:"injT"`
}

var _ = EventTimeExtractor(Message{})

func (m *Message) UpdateInjectTime(ts int64) {
	m.InjT = ts
}

func (m Message) ExtractInjectTimeMs() int64 {
	return m.InjT
}

func (m Message) ExtractEventTime() (int64, error) {
	return m.Timestamp, nil
}

func (m *Message) ExtractEventTimeFromVal() error {
	v := m.Value.(EventTimeExtractor)
	var err error
	m.Timestamp, err = v.ExtractEventTime()
	if err != nil {
		return err
	}
	return nil
}

func (m *Message) UpdateEventTime(ts int64) {
	m.Timestamp = ts
}

var EmptyMessage = Message{}

var EmptyRawMsg = RawMsg{Payload: nil, MsgSeqNum: 0, LogSeqNum: 0}

type RawMsg struct {
	Payload []byte

	MsgSeqNum  uint64
	LogSeqNum  uint64
	ScaleEpoch uint64

	ProdId ProducerId

	IsControl    bool
	IsPayloadArr bool
	Mark         EpochMark
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

func DecodeRawMsg(rawMsg *RawMsg, msgSerde MessageSerde,
	payloadArrSerde Serde,
) (*MsgAndSeq, error) {
	if rawMsg.IsPayloadArr {
		payloadArrTmp, err := payloadArrSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, err
		}
		payloadArr := payloadArrTmp.(PayloadArr)
		var msgArr []Message
		for _, payload := range payloadArr.Payloads {
			msgTmp, err := msgSerde.Decode(payload)
			if err != nil {
				return nil, err
			}
			msg := msgTmp.(Message)
			msgArr = append(msgArr, msg)
		}
		return &MsgAndSeq{MsgArr: msgArr, Msg: EmptyMessage,
			MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum}, nil
	} else {
		msgTmp, err := msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, err
		}
		msg := msgTmp.(Message)
		return &MsgAndSeq{
			Msg:       msg,
			MsgArr:    nil,
			MsgSeqNum: rawMsg.MsgSeqNum,
			LogSeqNum: rawMsg.LogSeqNum,
			IsControl: false,
		}, nil
	}
}

func ApplyFuncToMsgSeqs(msgSeqs *MsgAndSeqs, callback func(msg *Message) error) error {
	for _, msgSeq := range msgSeqs.Msgs {
		if msgSeq.MsgArr != nil {
			for _, msg := range msgSeq.MsgArr {
				if err := callback(&msg); err != nil {
					return err
				}
			}
		} else {
			if err := callback(&msgSeq.Msg); err != nil {
				return err
			}
		}
	}
	return nil
}
