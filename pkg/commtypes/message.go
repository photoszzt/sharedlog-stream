package commtypes

import "fmt"

type Punctuate struct{}

type Message[K, V any] struct {
	Key       K
	Value     V
	Timestamp int64
	InjT      int64
}

var _ = fmt.Stringer(Message[int, int]{})

func (m Message[K, V]) String() string {
	return fmt.Sprintf("Msg: {Key: %v, Value: %v, Ts: %d, InjectTs: %d}", m.Key, m.Value, m.Timestamp, m.InjT)
}

var _ = EventTimeExtractor(Message[int, int]{})

func (m *Message[K, V]) UpdateInjectTime(ts int64) {
	m.InjT = ts
}

func (m Message[K, V]) ExtractInjectTimeMs() int64 {
	return m.InjT
}

func (m Message[K, V]) ExtractEventTime() (int64, error) {
	return m.Timestamp, nil
}

func ExtractEventTimeFromVal[KeyT any, ValueT EventTimeExtractor](msg Message[KeyT, ValueT]) error {
	var err error
	msg.Timestamp, err = msg.Value.ExtractEventTime()
	if err != nil {
		return err
	}
	return nil
}

func (m *Message[K, V]) UpdateEventTime(ts int64) {
	m.Timestamp = ts
}

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

type MsgAndSeq[K, V any] struct {
	Msg       Message[K, V]
	MsgArr    []Message[K, V]
	MsgSeqNum uint64
	LogSeqNum uint64
	IsControl bool
}

type MsgAndSeqs[K, V any] struct {
	Msgs     []MsgAndSeq[K, V]
	TotalLen uint32
}

func DecodeRawMsg[K, V any](rawMsg *RawMsg, msgSerde MessageSerde[K, V],
	payloadArrSerde Serde[PayloadArr],
) (*MsgAndSeq[K, V], error) {
	if rawMsg.IsPayloadArr {
		payloadArr, err := payloadArrSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, fmt.Errorf("fail to decode payload arr: %v", err)
		}
		var msgArr []Message[K, V]
		for _, payload := range payloadArr.Payloads {
			msg, err := msgSerde.Decode(payload)
			if err != nil {
				return nil, fmt.Errorf("fail to decode msg 1: %v", err)
			}
			msgArr = append(msgArr, msg)
		}
		return &MsgAndSeq[K, V]{MsgArr: msgArr, Msg: Message[K, V]{},
			MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum}, nil
	} else {
		msg, err := msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return nil, fmt.Errorf("fail to decode msg 2: %v", err)
		}
		return &MsgAndSeq[K, V]{
			Msg:       msg,
			MsgArr:    nil,
			MsgSeqNum: rawMsg.MsgSeqNum,
			LogSeqNum: rawMsg.LogSeqNum,
			IsControl: false,
		}, nil
	}
}

func ApplyFuncToMsgSeqs[K, V any](msgSeqs *MsgAndSeqs[K, V], callback func(msg *Message[K, V]) error) error {
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
