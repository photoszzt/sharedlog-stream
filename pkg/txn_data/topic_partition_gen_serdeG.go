package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type TopicPartitionJSONSerdeG struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.SerdeG[*TopicPartition](TopicPartitionJSONSerdeG{})

func (s TopicPartitionJSONSerdeG) Encode(value *TopicPartition) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s TopicPartitionJSONSerdeG) Decode(value []byte) (*TopicPartition, error) {
	v := TopicPartition{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type TopicPartitionMsgpSerdeG struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.SerdeG[*TopicPartition](TopicPartitionMsgpSerdeG{})

func (s TopicPartitionMsgpSerdeG) Encode(value *TopicPartition) ([]byte, *[]byte, error) {
	b := commtypes.PopBuffer()
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s TopicPartitionMsgpSerdeG) Decode(value []byte) (*TopicPartition, error) {
	v := TopicPartition{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetTopicPartitionSerdeG(serdeFormat commtypes.SerdeFormat) (commtypes.SerdeG[*TopicPartition], error) {
	if serdeFormat == commtypes.JSON {
		return TopicPartitionJSONSerdeG{}, nil
	} else if serdeFormat == commtypes.MSGP {
		return TopicPartitionMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
