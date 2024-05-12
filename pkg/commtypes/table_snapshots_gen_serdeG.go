package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type TableSnapshotsJSONSerdeG struct {
	DefaultJSONSerde
}

var _ = SerdeG[TableSnapshots](TableSnapshotsJSONSerdeG{})

func (s TableSnapshotsJSONSerdeG) Encode(value TableSnapshots) ([]byte, *[]byte, error) {
	r, err := json.Marshal(value)
	return r, nil, err
}

func (s TableSnapshotsJSONSerdeG) Decode(value []byte) (TableSnapshots, error) {
	v := TableSnapshots{}
	if err := json.Unmarshal(value, &v); err != nil {
		return TableSnapshots{}, err
	}
	return v, nil
}

type TableSnapshotsMsgpSerdeG struct {
	DefaultMsgpSerde
}

var _ = SerdeG[TableSnapshots](TableSnapshotsMsgpSerdeG{})

func (s TableSnapshotsMsgpSerdeG) Encode(value TableSnapshots) ([]byte, *[]byte, error) {
	b := PopBuffer(value.Msgsize())
	buf := *b
	r, err := value.MarshalMsg(buf[:0])
	return r, b, err
}

func (s TableSnapshotsMsgpSerdeG) Decode(value []byte) (TableSnapshots, error) {
	v := TableSnapshots{}
	if _, err := v.UnmarshalMsg(value); err != nil {
		return TableSnapshots{}, err
	}
	return v, nil
}

func GetTableSnapshotsSerdeG(serdeFormat SerdeFormat) (SerdeG[TableSnapshots], error) {
	if serdeFormat == JSON {
		return TableSnapshotsJSONSerdeG{}, nil
	} else if serdeFormat == MSGP {
		return TableSnapshotsMsgpSerdeG{}, nil
	} else {
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
