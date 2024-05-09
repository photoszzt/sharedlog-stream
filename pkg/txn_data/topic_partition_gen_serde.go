package txn_data

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
)

type TopicPartitionJSONSerde struct {
	commtypes.DefaultJSONSerde
}

var _ = commtypes.Serde(TopicPartitionJSONSerde{})

func (s TopicPartitionJSONSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*TopicPartition)
	if !ok {
		vTmp := value.(TopicPartition)
		v = &vTmp
	}
	r, err := json.Marshal(v)
	return r, nil, err
}

func (s TopicPartitionJSONSerde) Decode(value []byte) (interface{}, error) {
	v := TopicPartition{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

type TopicPartitionMsgpSerde struct {
	commtypes.DefaultMsgpSerde
}

var _ = commtypes.Serde(TopicPartitionMsgpSerde{})

func (s TopicPartitionMsgpSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*TopicPartition)
	if !ok {
		vTmp := value.(TopicPartition)
		v = &vTmp
	}
	b := commtypes.PopBuffer()
	buf := *b
	r, err := v.MarshalMsg(buf[:0])
	return r, b, err
}

func (s TopicPartitionMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := TopicPartition{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return &v, nil
}

func GetTopicPartitionSerde(serdeFormat commtypes.SerdeFormat) (commtypes.Serde, error) {
	switch serdeFormat {
	case commtypes.JSON:
		return TopicPartitionJSONSerde{}, nil
	case commtypes.MSGP:
		return TopicPartitionMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
