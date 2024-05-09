package commtypes

import (
	"fmt"
)

var _ = fmt.Stringer(TableSnapshotsJSONSerde{})

func (s TableSnapshotsJSONSerde) String() string {
	return "TableSnapshotsJSONSerde"
}

var _ = fmt.Stringer(TableSnapshotsMsgpSerde{})

func (s TableSnapshotsMsgpSerde) String() string {
	return "TableSnapshotsMsgpSerde"
}
