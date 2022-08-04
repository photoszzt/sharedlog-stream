//go:generate msgp
//msgp:ignore ControlMetadataJSONSerde ControlMetadataMsgpSerde
package txn_data

type ControlMetadata struct {
	// number of instances for each stage
	Config map[string]uint8 `json:"sg,omitempty" msgp:"sg,omitempty"`

	// topic of the stream
	Topic string `json:"tp,omitempty" msgp:"tp,omitempty"`
	// old scale epoch done
	FinishedPrevTask string `json:"fpt,omitempty" msgp:"fpt,omitempty"`

	// key of msg
	Key []byte `json:"k,omitempty" msgp:"k,omitempty"`

	Epoch uint16 `json:"ep,omitempty" msgp:"ep,omitempty"`
	// old instance id
	InstanceId uint8 `json:"iid,omitempty" msgp:"iid,omitempty"`

	// substream id that the key stores to
	SubstreamId uint8 `json:"sid,omitempty" msgp:"sid,omitempty"`
}
