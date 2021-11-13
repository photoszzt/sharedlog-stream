package store

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

type InMemoryWindowStoreWithChangelog struct {
	bytesWindowStore *InMemoryBytesWindowStore
	mp               *MaterializeParam
	keyWindowTsSerde commtypes.Serde
}

var _ = WindowStore(&InMemoryWindowStoreWithChangelog{})

func NewInMemoryWindowStoreWithChangelog(retensionPeriod uint64, windowSize uint64, mp *MaterializeParam) (*InMemoryWindowStoreWithChangelog, error) {
	var ktsSerde commtypes.Serde
	if mp.serdeFormat == commtypes.JSON {
		ktsSerde = commtypes.KeyAndWindowStartTsJSONSerde{}
	} else if mp.serdeFormat == commtypes.MSGP {
		ktsSerde = commtypes.KeyAndWindowStartTsMsgpSerde{}
	} else {
		return nil, fmt.Errorf("serde format should be either json or msgp; but %v is given", mp.serdeFormat)
	}
	return &InMemoryWindowStoreWithChangelog{
		bytesWindowStore: NewInMemoryBytesWindowStore(mp.StoreName, retensionPeriod, windowSize, false, mp.ValueSerde),
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
			err = st.bytesWindowStore.Put(keyWin.Key, valBytes, keyWin.WindowStartTs)
			if err != nil {
				return err
			}
		}
	}
}

func (st *InMemoryWindowStoreWithChangelog) Init(ctx StoreContext) {
	st.bytesWindowStore.Init(ctx)
	ctx.RegisterWindowStore(st)
}

func (st *InMemoryWindowStoreWithChangelog) Name() string {
	return st.bytesWindowStore.name
}

func (st *InMemoryWindowStoreWithChangelog) Put(ctx context.Context, key KeyT, value ValueT, windowStartTimestamp uint64) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.mp.ValueSerde.Encode(value)
	if err != nil {
		return err
	}

	keyAndTs := commtypes.KeyAndWindowStartTs{
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
	err = st.bytesWindowStore.Put(keyBytes, valBytes, windowStartTimestamp)
	return err
}

func (st *InMemoryWindowStoreWithChangelog) Get(key KeyT, windowStartTimestamp uint64) (ValueT, bool, error) {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return nil, false, err
	}
	valBytes, ok := st.bytesWindowStore.Get(keyBytes, windowStartTimestamp)
	if !ok {
		return nil, false, nil
	}
	val, err := st.mp.ValueSerde.Decode(valBytes)
	if err != nil {
		return nil, false, err
	}
	return val, ok, nil
}

func (st *InMemoryWindowStoreWithChangelog) Fetch(key KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(uint64, ValueT) error) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	return st.bytesWindowStore.Fetch(keyBytes, timeFrom, timeTo, iterFunc)
}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetch(key KeyT, timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")

}

func (st *InMemoryWindowStoreWithChangelog) FetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")

}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetchWithKeyRange(keyFrom KeyT, keyTo KeyT, timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")

}

func (st *InMemoryWindowStoreWithChangelog) FetchAll(timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")

}

func (st *InMemoryWindowStoreWithChangelog) BackwardFetchAll(timeFrom time.Time, timeTo time.Time) {
	panic("not implemented")

}
