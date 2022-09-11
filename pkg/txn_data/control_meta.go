//go:generate msgp
//msgp:ignore ControlMetadataJSONSerde ControlMetadataMsgpSerde
package txn_data

type ControlMetadata struct {
	Config           map[string]uint8 `json:"sg,omitempty" msgp:"sg,omitempty"`
	FinishedPrevTask string           `json:"fpt,omitempty" msgp:"fpt,omitempty"`
	KeyMaps          []KeyMaping      `json:"km,omitempty" msgp:"km,omitempty"`
	Epoch            uint16           `json:"ep,omitempty" msgp:"ep,omitempty"`
	InstanceId       uint8            `json:"iid,omitempty" msgp:"iid,omitempty"`
}

type KeyMaping struct {
	Topic       string `json:"tp,omitempty" msgp:"tp,omitempty"`
	Key         []byte `json:"k,omitempty" msgp:"k,omitempty"`
	Hash        uint64 `json:"h,omitempty" msgp:"h,omitempty"`
	SubstreamId uint8  `json:"sid,omitempty" msgp:"sid,omitempty"`
}
