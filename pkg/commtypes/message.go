package commtypes

import "fmt"

const (
	END_OF_STREAM_KEY = "__end_stream"
)

type Punctuate struct{}

type MessageSerde interface {
	Serde
	SerdeG[interface{}]
	EncodeAndRtnKVBin(value interface{}) ([]byte, []byte /* kEnc */, []byte /* vEnc */, error)
	EncodeKey(key interface{}) ([]byte, error)
	EncodeVal(val interface{}) ([]byte, error)
	DecodeVal(value []byte) (interface{}, error)
}

type Message struct {
	Key       interface{}
	Value     interface{}
	Timestamp int64
	InjT      int64
}

var _ = fmt.Stringer(Message{})

func (m Message) String() string {
	return fmt.Sprintf("Msg: {Key: %v, Value: %v, Ts: %d, InjectTs: %d}", m.Key, m.Value, m.Timestamp, m.InjT)
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

	MsgSeqNum uint64
	LogSeqNum uint64
	StartTime int64

	ProdId ProducerId

	ScaleEpoch   uint16
	IsControl    bool
	IsPayloadArr bool
	ProdIdx      uint8
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
	Msgs     *MsgAndSeq
	TotalLen uint32
}

func DecodeRawMsg(rawMsg *RawMsg, msgSerde MessageSerde,
	payloadArrSerde Serde,
) (*MsgAndSeq, error) {
	if rawMsg.IsPayloadArr {
		payloadArrTmp, err := payloadArrSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, fmt.Errorf("fail to decode payload arr: %v", err)
		}
		payloadArr := payloadArrTmp.(PayloadArr)
		msgArr := make([]Message, 0, len(payloadArr.Payloads))
		for _, payload := range payloadArr.Payloads {
			msgTmp, err := msgSerde.Decode(payload)
			if err != nil {
				return nil, fmt.Errorf("fail to decode msg 1: %v", err)
			}
			msg := msgTmp.(Message)
			msgArr = append(msgArr, msg)
		}
		return &MsgAndSeq{MsgArr: msgArr, Msg: EmptyMessage,
			MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum}, nil
	} else {
		msgTmp, err := msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, fmt.Errorf("fail to decode msg 2: %v", err)
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

func DecodeRawMsgG[K, V any](rawMsg *RawMsg, msgSerde MessageSerdeG[K, V],
	payloadArrSerde SerdeG[PayloadArr],
) (*MsgAndSeq, error) {
	if rawMsg.IsPayloadArr {
		payloadArr, err := payloadArrSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, fmt.Errorf("fail to decode payload arr: %v", err)
		}
		msgArr := make([]Message, 0, len(payloadArr.Payloads))
		for _, payload := range payloadArr.Payloads {
			msg, err := msgSerde.Decode(payload)
			if err != nil {
				return nil, fmt.Errorf("fail to decode msg 1: %v", err)
			}
			msgArr = append(msgArr, msg)
		}
		return &MsgAndSeq{MsgArr: msgArr, Msg: EmptyMessage,
			MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum}, nil
	} else {
		msg, err := msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, fmt.Errorf("fail to decode msg 2: %v", err)
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

func ApplyFuncToMsgSeqs(msgSeqs *MsgAndSeqs, callback func(msg *Message) error) error {
	if msgSeqs.Msgs.MsgArr != nil {
		for _, msg := range msgSeqs.Msgs.MsgArr {
			if err := callback(&msg); err != nil {
				return err
			}
		}
	} else {
		if err := callback(&msgSeqs.Msgs.Msg); err != nil {
			return err
		}
	}
	return nil
}
