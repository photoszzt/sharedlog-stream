package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type TableSnapshotsJSONSerde struct {
	DefaultJSONSerde
}

var _ = Serde(TableSnapshotsJSONSerde{})

func (s TableSnapshotsJSONSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*TableSnapshots)
	if !ok {
		vTmp := value.(TableSnapshots)
		v = &vTmp
	}
	r, err := json.Marshal(v)
	return r, nil, err
}

func (s TableSnapshotsJSONSerde) Decode(value []byte) (interface{}, error) {
	v := TableSnapshots{}
	if err := json.Unmarshal(value, &v); err != nil {
		return nil, err
	}
	return v, nil
}

type TableSnapshotsMsgpSerde struct {
	DefaultMsgpSerde
}

var _ = Serde(TableSnapshotsMsgpSerde{})

func (s TableSnapshotsMsgpSerde) Encode(value interface{}) ([]byte, *[]byte, error) {
	v, ok := value.(*TableSnapshots)
	if !ok {
		vTmp := value.(TableSnapshots)
		v = &vTmp
	}
	b := PopBuffer()
	buf := *b
	r, err := v.MarshalMsg(buf[:0])
	return r, b, err
}

func (s TableSnapshotsMsgpSerde) Decode(value []byte) (interface{}, error) {
	v := TableSnapshots{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return nil, err
	}
	return v, nil
}

func GetTableSnapshotsSerde(serdeFormat SerdeFormat) (Serde, error) {
	switch serdeFormat {
	case JSON:
		return TableSnapshotsJSONSerde{}, nil
	case MSGP:
		return TableSnapshotsMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}