package processor

import "time"

type InMemoryWindowStoreWithChangelog struct {
	bytesWindowStore *InMemoryBytesWindowStore
	mp               *MaterializeParam
}

var _ = WindowStore(&InMemoryWindowStoreWithChangelog{})

func NewInMemoryWindowStoreWithChangelog(retensionPeriod uint64, windowSize uint64, mp *MaterializeParam) *InMemoryWindowStoreWithChangelog {
	return &InMemoryWindowStoreWithChangelog{
		bytesWindowStore: NewInMemoryBytesWindowStore(mp.StoreName, retensionPeriod, windowSize, false, mp.ValueSerde),
		mp:               mp,
	}
}

func (st *InMemoryWindowStoreWithChangelog) Init(ctx ProcessorContext) {
	st.bytesWindowStore.Init(ctx)
	ctx.RegisterWindowStore(st)
}

func (st *InMemoryWindowStoreWithChangelog) Name() string {
	return st.bytesWindowStore.name
}

func (st *InMemoryWindowStoreWithChangelog) Put(key KeyT, value ValueT, windowStartTimestamp uint64) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	valBytes, err := st.mp.ValueSerde.Encode(value)
	if err != nil {
		return err
	}
	return st.bytesWindowStore.Put(keyBytes, valBytes, windowStartTimestamp)
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

func (st *InMemoryWindowStoreWithChangelog) Fetch(key KeyT, timeFrom time.Time, timeTo time.Time, iterFunc func(uint64, ValueT)) error {
	keyBytes, err := st.mp.KeySerde.Encode(key)
	if err != nil {
		return err
	}
	st.bytesWindowStore.Fetch(keyBytes, timeFrom, timeTo, iterFunc)
	return nil
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
