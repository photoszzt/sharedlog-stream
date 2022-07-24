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
type TopicPartitionJSONSerdeG struct{}

var _ = commtypes.Serde(TopicPartitionJSONSerde{})
var _ = commtypes.SerdeG[TopicPartition](TopicPartitionJSONSerdeG{})

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

func (s TopicPartitionJSONSerdeG) Encode(value TopicPartition) ([]byte, error) {
	return json.Marshal(&value)
}

func (s TopicPartitionJSONSerdeG) Decode(value []byte) (TopicPartition, error) {
	tp := TopicPartition{}
	if err := json.Unmarshal(value, &tp); err != nil {
		return TopicPartition{}, err
	}
	return tp, nil
}

type TopicPartitionMsgpSerde struct{}
type TopicPartitionMsgpSerdeG struct{}

var _ = commtypes.Serde(TopicPartitionMsgpSerde{})
var _ = commtypes.SerdeG[TopicPartition](TopicPartitionMsgpSerdeG{})

func (s TopicPartitionMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tp := value.(*TopicPartition)
	return tp.UnmarshalMsg(nil)
}
func (s TopicPartitionMsgpSerdeG) Encode(value TopicPartition) ([]byte, error) {
	return value.UnmarshalMsg(nil)
}

func (s TopicPartitionMsgpSerde) Decode(value []byte) (interface{}, error) {
	tp := TopicPartition{}
	if _, err := tp.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return tp, nil
}

func (s TopicPartitionMsgpSerdeG) Decode(value []byte) (TopicPartition, error) {
	tp := TopicPartition{}
	if _, err := tp.UnmarshalMsg(value); err != nil {
		return TopicPartition{}, err
	}
	return tp, nil
}
