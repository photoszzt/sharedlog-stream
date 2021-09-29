//go:generate msgp
package wordcount

import "encoding/json"

type WordEvent struct {
	Word string `json:"word" msgp:"word"`
	Ts   uint64 `json:"ts" msgp:"ts"`
}

type WordEventJSONSerde struct{}

func (s WordEventJSONSerde) Encode(value interface{}) ([]byte, error) {
	w := value.(*WordEvent)
	return json.Marshal(w)
}

func (s WordEventJSONSerde) Decode(value []byte) (interface{}, error) {
	w := WordEvent{}
	if err := json.Unmarshal(value, &w); err != nil {
		return nil, err
	}
	return &w, nil
}

type WordEventMsgpSerde struct{}

func (s WordEventMsgpSerde) Encode(value interface{}) ([]byte, error) {
	w := value.(*WordEvent)
	return w.MarshalMsg(nil)
}

func (s WordEventMsgpSerde) Decode(value []byte) (interface{}, error) {
	w := WordEvent{}
	if _, err := w.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &w, nil
}

type SentenceEvent struct {
	Sentence string `json:"sen" msgp:"sen"`
	Ts       uint64 `json:"ts" msgp:"ts"`
}

type SentenceEventJSONSerde struct{}

func (s SentenceEventJSONSerde) Encode(value interface{}) ([]byte, error) {
	se := value.(*SentenceEvent)
	return json.Marshal(se)
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
	se := value.(*SentenceEvent)
	return se.MarshalMsg(nil)
}

func (s SentenceEventMsgpSerde) Decode(value []byte) (interface{}, error) {
	se := SentenceEvent{}
	if _, err := se.UnmarshalMsg(nil); err != nil {
		return nil, err
	}
	return &se, nil
}
