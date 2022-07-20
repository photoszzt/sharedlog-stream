//go:generate msgp
//msgp:ignore TopicPartitionJSONSerde TopicPartitionMsgpSerde
package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/commtypes"
)

type TopicPartition struct {
	Topic  string  `json:"topic" msg:"topic"`
	ParNum []uint8 `json:"parnum" msg:"parnum"`
}

type TopicPartitionJSONSerde struct{}

var _ = commtypes.Serde[TopicPartition](TopicPartitionJSONSerde{})

func (s TopicPartitionJSONSerde) Encode(value TopicPartition) ([]byte, error) {
	return json.Marshal(&value)
}

func (s TopicPartitionJSONSerde) Decode(value []byte) (TopicPartition, error) {
	tp := TopicPartition{}
	if err := json.Unmarshal(value, &tp); err != nil {
		return TopicPartition{}, err
	}
	return tp, nil
}

type TopicPartitionMsgpSerde struct{}

var _ = commtypes.Serde[TopicPartition](TopicPartitionMsgpSerde{})

func (s TopicPartitionMsgpSerde) Encode(value TopicPartition) ([]byte, error) {
	return value.UnmarshalMsg(nil)
}

func (s TopicPartitionMsgpSerde) Decode(value []byte) (TopicPartition, error) {
	tp := TopicPartition{}
	if _, err := tp.UnmarshalMsg(value); err != nil {
		return TopicPartition{}, err
	}
	return tp, nil
}
