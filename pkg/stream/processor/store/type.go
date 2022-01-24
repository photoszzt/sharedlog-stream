//go:generate msgp
//msgp:ignore ValueTimestamp Change ValueTimestampJSONSerde ValueTimestampMsgpSerde
package store

type KeyT interface{}

type ValueT interface{}

type versionedKey struct {
	key     interface{}
	version uint32
}

type versionedKeySerialized struct {
	key     []byte `msgp:"k,omitempty",json:"k,omitempty"`
	version uint32 `msgp:"ver,omitempty",json:"ver,omitempty"`
}
