package commtypes

import (
	"encoding/json"
	"fmt"
	"os"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/debug"
)

func GetTableSnapshotsSerde(serdeFormat SerdeFormat) (SerdeG[TableSnapshots], error) {
	debug.Fprintf(os.Stderr, "serdeFormat for table snapshots serde: %v\n", serdeFormat)
	switch serdeFormat {
	case JSON:
		return TableSnapshotsJSONSerde{}, nil
	case MSGP:
		return TableSnapshotsMsgpSerde{}, nil
	default:
		return nil, common_errors.ErrUnrecognizedSerdeFormat
	}
}

type TableSnapshotsJSONSerde struct{}

var _ = fmt.Stringer(TableSnapshotsJSONSerde{})

func (s TableSnapshotsJSONSerde) String() string {
	return "TableSnapshotsJSONSerde"
}

var _ SerdeG[TableSnapshots] = TableSnapshotsJSONSerde{}

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

var _ = fmt.Stringer(TableSnapshotsMsgpSerde{})

func (s TableSnapshotsMsgpSerde) String() string {
	return "TableSnapshotsMsgpSerde"
}

var _ SerdeG[TableSnapshots] = TableSnapshotsMsgpSerde{}

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
