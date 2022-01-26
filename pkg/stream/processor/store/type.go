//go:generate msgp
//msgp:ignore ValueTimestamp Change ValueTimestampJSONSerde ValueTimestampMsgpSerde
package store

type KeyT interface{}

type ValueT interface{}

type VersionedKey struct {
	Key     interface{}
	Version uint32
}

type versionedKeySerialized struct {
	key     []byte `msgp:"k,omitempty",json:"k,omitempty"`
	version uint32 `msgp:"ver,omitempty",json:"ver,omitempty"`
}
