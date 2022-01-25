package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type InMemoryWindowStoreWithChangelog struct {
	windowStore      *InMemoryWindowStore
	mp               *MaterializeParam
	keyWindowTsSerde commtypes.Serde
}

var _ = WindowStore(&InMemoryWindowStoreWithChangelog{})

func NewInMemoryWindowStoreWithChangelog(retensionPeriod int64, windowSize int64, mp *MaterializeParam) (*InMemoryWindowStoreWithChangelog, error) {
	var ktsSerde commtypes.Serde
	if mp.SerdeFormat == commtypes.JSON {
		ktsSerde = commtypes.KeyAndWindowStartTsJSONSerde{}
	} else if mp.SerdeFormat == commtypes.MSGP {
		ktsSerde = commtypes.KeyAndWindowStartTsMsgpSerde{}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", mp.SerdeFormat)
	}
	return &InMemoryWindowStoreWithChangelog{
		windowStore:      NewInMemoryWindowStore(mp.StoreName, retensionPeriod, windowSize, false, mp.Comparable),
		mp:               mp,
		keyWindowTsSerde: ktsSerde,
	}, nil
}

func (st *InMemoryWindowStoreWithChangelog) RestoreStateStore(ctx context.Context) error {
	for {
		_, msgs, err := st.mp.Changelog.ReadNext(ctx, st.mp.ParNum)
		// nothing to restore
		if errors.IsStreamEmptyError(err) {
			return nil
		} else if err != nil {
			return err
		}
		for _, msg := range msgs {
			keyWinBytes, valBytes, err := st.mp.MsgSerde.Decode(msg.Payload)
			if err != nil {
				return err
			}
			keyWinTmp, err := st.keyWindowTsSerde.Decode(keyWinBytes)
			if err != nil {
				return err
			}
			keyWin := keyWinTmp.(commtypes.KeyAndWindowStartTs)
			err = st.windowStore.Put(keyWin.Key, valBytes, keyWin.WindowStartTs)
			if err != nil {
				return err
			}
		}
	}
}

func (st *InMemoryWindowStoreWithChangelog) Init(ctx StoreContext) {
	st.windowStore.Init(ctx)
	ctx.RegisterWindowStore(st)
}

func (st *InMemoryWindowStoreWithChangelog) Name() string {
	return st.windowStore.name
}

func (st *InMemoryWindowStoreWithChangelog) Put(ctx context.Context, key KeyT, value ValueT, windowStartTimestamp int64) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.mp.ValueSerde.Encode(value)
	if err != nil {
		return err
	}

	keyAndTs := &commtypes.KeyAndWindowStartTs{
		Key:           keyBytes,
		WindowStartTs: windowStartTimestamp,
	}
	ktsBytes, err := st.keyWindowTsSerde.Encode(keyAndTs)
	if err != nil {
		return err
	}
	encoded, err := st.mp.MsgSerde.Encode(ktsBytes, valBytes)
	if err != nil {
		return err
	}
	_, err = st.mp.Changelog.Push(ctx, encoded, st.mp.ParNum, false)
	if err != nil {
		return err
	}
	err = st.windowStore.Put(key, value, windowStartTimestamp)
	return err
}

func (st *InMemoryWindowStoreWithChangelog) Get(key KeyT, windowStartTimestamp int64) (ValueT, bool, error) {
	val, ok := st.windowStore.Get(key, windowStartTimestamp)
	if !ok {
		return nil, false, nil
	}
	return val, ok, nil
}

func (st *InMemoryWindowStoreWithChangelog) Fetch(
	key KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	return st.windowStore.Fetch(key, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetch(
	key KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")

}

func (st *InMemoryWindowStoreWithChangelog) FetchWithKeyRange(
	keyFrom KeyT,
	keyTo KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")
}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetchWithKeyRange(
	keyFrom KeyT,
	keyTo KeyT,
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")

}

func (st *InMemoryWindowStoreWithChangelog) FetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")

}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetchAll(
	timeFrom time.Time,
	timeTo time.Time,
	iterFunc func(int64, KeyT, ValueT) error,
) error {
	panic("not implemented")

}
