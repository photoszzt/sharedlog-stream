package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/commtypes"
)

type (
	TopicPartitionJSONSerde struct {
		commtypes.DefaultJSONSerde
	}
	TopicPartitionJSONSerdeG struct {
		commtypes.DefaultJSONSerde
	}
)

var (
	_ = commtypes.Serde(TopicPartitionJSONSerde{})
	_ = commtypes.SerdeG[*TopicPartition](TopicPartitionJSONSerdeG{})
)

func (s TopicPartitionJSONSerde) Encode(value interface{}) ([]byte, error) {
	tp := value.(*TopicPartition)
	return json.Marshal(tp)
}

func (s TopicPartitionJSONSerde) Decode(value []byte) (interface{}, error) {
	tp := &TopicPartition{}
	if err := json.Unmarshal(value, &tp); err != nil {
		return nil, err
	}
	return tp, nil
}

func (s TopicPartitionJSONSerdeG) Encode(value *TopicPartition) ([]byte, error) {
	return json.Marshal(&value)
}

func (s TopicPartitionJSONSerdeG) Decode(value []byte) (*TopicPartition, error) {
	tp := TopicPartition{}
	if err := json.Unmarshal(value, &tp); err != nil {
		return nil, err
	}
	return &tp, nil
}

type (
	TopicPartitionMsgpSerde struct {
		commtypes.DefaultMsgpSerde
	}
	TopicPartitionMsgpSerdeG struct {
		commtypes.DefaultMsgpSerde
	}
)

var (
	_ = commtypes.Serde(TopicPartitionMsgpSerde{})
	_ = commtypes.SerdeG[*TopicPartition](TopicPartitionMsgpSerdeG{})
)

func (s TopicPartitionMsgpSerde) Encode(value interface{}) ([]byte, error) {
	tp := value.(*TopicPartition)
	b := commtypes.PopBuffer()
	buf := *b
	return tp.MarshalMsg(buf[:0])
}

func (s TopicPartitionMsgpSerdeG) Encode(value *TopicPartition) ([]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	return value.MarshalMsg(buf[:0])
}

func (s TopicPartitionMsgpSerde) Decode(value []byte) (interface{}, error) {
	tp := TopicPartition{}
	if _, err := tp.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &tp, nil
}

func (s TopicPartitionMsgpSerdeG) Decode(value []byte) (*TopicPartition, error) {
	tp := TopicPartition{}
	if _, err := tp.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &tp, nil
}
