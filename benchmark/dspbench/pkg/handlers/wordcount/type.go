package wordcount

//go:generate msgp
//msgp:ignore SentenceEventJSONSerde SentenceEventMsgpSerde
import "encoding/json"

type SentenceEvent struct {
	Sentence string `json:"sen" msg:"sen"`
	Ts       uint64 `json:"ts" msg:"ts"`
}

type SentenceEventJSONSerde struct{}

func (s SentenceEventJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*SentenceEvent)
	return json.Marshal(val)
}

func (s SentenceEventJSONSerde) Decode(value []byte) (interface{}, error) {
	se := SentenceEvent{}
	if err := json.Unmarshal(value, &se); err != nil {
		return nil, err
	}
	return &se, nil
}

type SentenceEventMsgpSerde struct{}

func (s SentenceEventMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*SentenceEvent)
	return val.MarshalMsg(nil)
}

func (s SentenceEventMsgpSerde) Decode(value []byte) (interface{}, error) {
	se := SentenceEvent{}
	if _, err := se.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &se, nil
}

type WordEvent struct {
	Word string `json:"word" msg:"word"`
	Ts   uint64 `json:"ts" msg:"ts"`
}

type WordEventJSONSerde struct{}

func (s WordEventJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*WordEvent)
	return json.Marshal(val)
}

func (s WordEventJSONSerde) Decode(value []byte) (interface{}, error) {
	we := WordEvent{}
	if err := json.Unmarshal(value, &we); err != nil {
		return nil, err
	}
	return &we, nil
}

type WordEventMsgpSerde struct{}

func (s WordEventMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*WordEvent)
	return val.MarshalMsg(nil)
}

func (s WordEventMsgpSerde) Decode(value []byte) (interface{}, error) {
	we := WordEvent{}
	if _, err := we.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &we, nil
}

type CountEvent struct {
	Count uint64 `json:"count" msg:"count"`
	Ts    uint64 `json:"ts" msg:"ts"`
}

type CountEventJSONSerde struct{}

func (s CountEventJSONSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*CountEvent)
	return json.Marshal(val)
}

func (s CountEventJSONSerde) Decode(value []byte) (interface{}, error) {
	ce := CountEvent{}
	if err := json.Unmarshal(value, &ce); err != nil {
		return nil, err
	}
	return &ce, nil
}

type CountEventMsgpSerde struct{}

func (s CountEventMsgpSerde) Encode(value interface{}) ([]byte, error) {
	val := value.(*CountEvent)
	return val.MarshalMsg(nil)
}

func (s CountEventMsgpSerde) Decode(value []byte) (interface{}, error) {
	ce := CountEvent{}
	if _, err := ce.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &ce, nil
}
