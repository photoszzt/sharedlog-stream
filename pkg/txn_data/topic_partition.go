//go:generate msgp
//msgp:ignore TopicPartitionJSONSerde TopicPartitionMsgpSerde
package txn_data

import "encoding/json"

type TopicPartition struct {
	Topic  string  `json:"topic" msg:"topic"`
	ParNum []uint8 `json:"parnum" msg:"parnum"`
}

type TopicPartitionJSONSerde struct{}

func (s TopicPartitionJSONSerde) Encode(value interface{}) ([]byte, error) {
	tp := value.(*TopicPartition)
	return json.Marshal(tp)
}

func (s TopicPartitionJSONSerde) Decode(value []byte) (interface{}, error) {
	tp := TopicPartition{}
	if err := json.Unmarshal(value, &tp); err != nil {
		return nil, err
	}
	return tp, nil
}

type TopicPartitionMsgpSerde struct{}

func (s TopicPartitionMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tp := value.(*TopicPartition)
	return tp.UnmarshalMsg(nil)
}

func (s TopicPartitionMsgpSerde) Decode(value []byte) (interface{}, error) {
	tp := TopicPartition{}
	if _, err := tp.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return tp, nil
}
