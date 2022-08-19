package store

type TABLE_TYPE uint8

const (
	IN_MEM  TABLE_TYPE = 0
	MONGODB TABLE_TYPE = 1
)

type VersionedKey struct {
	Key     interface{}
	Version uint32
}

type VersionedKeyG[K any] struct {
	Key     K
	Version uint32
}

/*
type versionedKeySerialized struct {
	key     []byte `msgp:"k,omitempty",json:"k,omitempty"`
	version uint32 `msgp:"ver,omitempty",json:"ver,omitempty"`
}
*/
