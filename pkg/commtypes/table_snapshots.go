package commtypes

import (
	"encoding/json"
	"sharedlog-stream/pkg/common_errors"
)

type TableSnapshots struct {
	TabMaps map[string][][]byte `json:"tab_maps" msg:"tab_maps"`
}

type TableSnapshotsJSONSerde struct{}

func (s TableSnapshotsJSONSerde) Encode(v TableSnapshots) ([]byte, error) {
	return json.Marshal(v)
}

func (s TableSnapshotsJSONSerde) Decode(v []byte) (TableSnapshots, error) {
	var t TableSnapshots
	if err := json.Unmarshal(v, &t); err != nil {
		return TableSnapshots{}, err
	}
	return t, nil
}

type TableSnapshotsMsgpSerde struct{}

func (s TableSnapshotsMsgpSerde) Encode(v TableSnapshots) ([]byte, error) {
	return v.MarshalMsg(nil)
}

func (s TableSnapshotsMsgpSerde) Decode(v []byte) (TableSnapshots, error) {
	var t TableSnapshots
	if _, err := t.UnmarshalMsg(v); err != nil {
		return TableSnapshots{}, err
	}
	return t, nil
}

func GetTableSnapshotsSerde(serdeFormat SerdeFormat) (SerdeG[TableSnapshots], error) {
	switch serdeFormat {
	case JSON:
		return TableSnapshotsJSONSerde{}, nil
	case MSGP:
		return TableSnapshotsMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}
