package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

type KeyValueStore interface {
	StateStore
	Init(ctx StoreContext)
	Get(ctx context.Context, key commtypes.KeyT) (commtypes.ValueT, bool, error)
	Range(ctx context.Context, from commtypes.KeyT, to commtypes.KeyT,
		iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error
	ReverseRange(from commtypes.KeyT, to commtypes.KeyT, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error
	PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func(commtypes.KeyT, commtypes.ValueT) error) error
	ApproximateNumEntries(ctx context.Context) (uint64, error)
	Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) error
	PutIfAbsent(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT) (commtypes.ValueT, error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key commtypes.KeyT) error
	TableType() TABLE_TYPE
	StartTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error
	AbortTransaction(ctx context.Context) error
	GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error)
}

type Segment interface {
	StateStore
	Init(ctx StoreContext)
	Get(ctx context.Context, key []byte) ([]byte, bool, error)
	Range(ctx context.Context, from []byte, to []byte,
		iterFunc func([]byte, []byte) error) error
	ReverseRange(from []byte, to []byte, iterFunc func([]byte, []byte) error) error
	PrefixScan(prefix interface{}, prefixKeyEncoder commtypes.Encoder, iterFunc func([]byte, []byte) error) error
	ApproximateNumEntries(ctx context.Context) (uint64, error)
	Put(ctx context.Context, key []byte, value []byte) error
	PutIfAbsent(ctx context.Context, key []byte, value []byte) ([]byte, error)
	PutAll(context.Context, []*commtypes.Message) error
	Delete(ctx context.Context, key []byte) error

	Destroy(ctx context.Context) error
	DeleteRange(ctx context.Context, keyFrom interface{}, keyTo interface{}) error
}
