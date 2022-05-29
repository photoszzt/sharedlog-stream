package store

import (
	"context"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"time"
)

type WindowStore interface {
	StateStore
	Init(ctx StoreContext)
	Put(ctx context.Context, key commtypes.KeyT, value commtypes.ValueT, windowStartTimestamp int64) error
	Get(ctx context.Context, key commtypes.KeyT, windowStartTimestamp int64) (commtypes.ValueT, bool, error)
	Fetch(ctx context.Context, key commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64 /* ts */, commtypes.KeyT, commtypes.ValueT) error) error
	BackwardFetch(key commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	FetchWithKeyRange(ctx context.Context, keyFrom commtypes.KeyT,
		keyTo commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	BackwardFetchWithKeyRange(keyFrom commtypes.KeyT, keyTo commtypes.KeyT, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	FetchAll(ctx context.Context, timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	BackwardFetchAll(timeFrom time.Time, timeTo time.Time,
		iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	IterAll(iterFunc func(int64, commtypes.KeyT, commtypes.ValueT) error) error
	DropDatabase(ctx context.Context) error
	TableType() TABLE_TYPE
	StartTransaction(ctx context.Context) error
	CommitTransaction(ctx context.Context, taskRepr string, transactionID uint64) error
	AbortTransaction(ctx context.Context) error
	GetTransactionID(ctx context.Context, taskRepr string) (uint64, bool, error)
	SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc)
}
