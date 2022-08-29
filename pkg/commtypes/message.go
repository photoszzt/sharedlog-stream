package commtypes

import (
	"fmt"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/utils"
)

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

type MessageG[K, V any] struct {
	Key       optional.Option[K]
	Value     optional.Option[V]
	Timestamp int64
	InjT      int64
}

var _ = fmt.Stringer(Message{})

func (m MessageG[K, V]) String() string {
	return fmt.Sprintf("Msg: {Key: %v, Value: %v, Ts: %d, InjectTs: %d}", m.Key, m.Value, m.Timestamp, m.InjT)
}

func (m *MessageG[K, V]) UpdateInjectTime(ts int64) {
	m.InjT = ts
}

func (m MessageG[K, V]) ExtractInjectTimeMs() int64 {
	return m.InjT
}

func (m MessageG[K, V]) ExtractEventTime() (int64, error) {
	return m.Timestamp, nil
}

func (m *MessageG[K, V]) ExtractEventTimeFromVal() error {
	msg := Message{Value: m.Value.Unwrap()}
	v := msg.Value.(EventTimeExtractor)
	var err error
	m.Timestamp, err = v.ExtractEventTime()
	if err != nil {
		return err
	}
	return nil
}

type Message struct {
	Key       interface{}
	Value     interface{}
	Timestamp int64
	InjT      int64
}

func MessageToMessageG[K, V any](msg Message) MessageG[K, V] {
	k := optional.None[K]()
	v := optional.None[V]()
	if !utils.IsNil(msg.Key) {
		k = optional.Some(msg.Key.(K))
	}
	if !utils.IsNil(msg.Value) {
		v = optional.Some(msg.Value.(V))
	}
	return MessageG[K, V]{Key: k, Value: v, Timestamp: msg.Timestamp, InjT: msg.InjT}
}

func MessageGToMessage[K, V any](msgG MessageG[K, V]) Message {
	var k interface{}
	var v interface{}
	k, ok := msgG.Key.Take()
	if !ok {
		k = nil
	}
	v, ok = msgG.Value.Take()
	if !ok {
		v = nil
	}
	return Message{Key: k, Value: v, Timestamp: msgG.Timestamp, InjT: msgG.InjT}
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

type RawMsgAndSeq struct {
	Payload    []byte
	PayloadArr [][]byte
	MsgSeqNum  uint64
	LogSeqNum  uint64
	StartTime  int64
	ScaleEpoch uint16
	IsControl  bool
	Mark       EpochMark
	ProdIdx    uint8
}

type ControlParam struct {
	StartTime  int64
	ScaleEpoch uint16
	Mark       EpochMark
	ProdIdx    uint8
}

type MsgAndSeqG[K, V any] struct {
	Msg       MessageG[K, V]
	MsgArr    []MessageG[K, V]
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

func DecodeRawMsgG[K, V any](rawMsg *RawMsg, msgSerde MessageGSerdeG[K, V],
	payloadArrSerde SerdeG[PayloadArr],
) (MsgAndSeqG[K, V], error) {
	if rawMsg.IsPayloadArr {
		payloadArr, err := payloadArrSerde.Decode(rawMsg.Payload)
		if err != nil {
			return MsgAndSeqG[K, V]{}, fmt.Errorf("fail to decode payload arrG: %v", err)
		}
		msgArr := make([]MessageG[K, V], 0, len(payloadArr.Payloads))
		for _, payload := range payloadArr.Payloads {
			msg, err := msgSerde.Decode(payload)
			if err != nil {
				return MsgAndSeqG[K, V]{}, fmt.Errorf("fail to decode msgG 1: %v, serde is %v", err, msgSerde)
			}
			msgArr = append(msgArr, msg)
		}
		return MsgAndSeqG[K, V]{MsgArr: msgArr, Msg: MessageG[K, V]{},
			MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum}, nil
	} else {
		msg, err := msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return MsgAndSeqG[K, V]{}, fmt.Errorf("fail to decode msg 2: %v", err)
		}
		return MsgAndSeqG[K, V]{
			Msg:       msg,
			MsgArr:    nil,
			MsgSeqNum: rawMsg.MsgSeqNum,
			LogSeqNum: rawMsg.LogSeqNum,
			IsControl: false,
		}, nil
	}
}

func DecodeRawMsgSeqG[K, V any](rawMsg RawMsgAndSeq, msgSerde MessageGSerdeG[K, V]) (MsgAndSeqG[K, V], error) {
	if rawMsg.PayloadArr != nil {
		msgArr := make([]MessageG[K, V], 0, len(rawMsg.PayloadArr))
		for _, payload := range rawMsg.PayloadArr {
			msg, err := msgSerde.Decode(payload)
			if err != nil {
				return MsgAndSeqG[K, V]{}, fmt.Errorf("fail to decode msg 1: %v, serde: %+v", err, msgSerde)
			}
			msgArr = append(msgArr, msg)
		}
		return MsgAndSeqG[K, V]{MsgArr: msgArr, Msg: MessageG[K, V]{},
			MsgSeqNum: rawMsg.MsgSeqNum, LogSeqNum: rawMsg.LogSeqNum}, nil
	} else {
		msg, err := msgSerde.Decode(rawMsg.Payload)
		if err != nil {
			return MsgAndSeqG[K, V]{}, fmt.Errorf("fail to decode msg 2: %v", err)
		}
		return MsgAndSeqG[K, V]{
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
