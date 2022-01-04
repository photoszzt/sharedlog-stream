package store

import (
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
	"time"
)

const (
	WINDOW_SIZE      = int64(3)
	SEGMENT_INTERVAL = int64(60_000)
	RETENTION_PERIOD = 2 * SEGMENT_INTERVAL
)

func getWindowStore() *InMemoryBytesWindowStore {
	store := NewInMemoryBytesWindowStore("test1", RETENTION_PERIOD, WINDOW_SIZE, false,
		commtypes.StringSerde{})
	return store
}

func putKV(store *InMemoryBytesWindowStore, key uint32, val string, ts int64, keySerde commtypes.Serde, valSerde commtypes.Serde) error {
	kBytes, err := keySerde.Encode(key)
	if err != nil {
		return err
	}
	vBytes, err := valSerde.Encode(val)
	if err != nil {
		return err
	}
	err = store.Put(kBytes, vBytes, ts)
	return err
}

func putFirstBatch(store *InMemoryBytesWindowStore, startTime int64) error {
	iSerde := commtypes.Uint32Serde{}
	sSerde := commtypes.StringSerde{}
	err := putKV(store, 0, "zero", startTime, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 1, "one", startTime+1, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 2, "two", startTime+2, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 4, "four", startTime+4, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 5, "five", startTime+5, iSerde, sSerde)
	return err
}

func putSecondBatch(store *InMemoryBytesWindowStore, startTime int64) error {
	iSerde := commtypes.Uint32Serde{}
	sSerde := commtypes.StringSerde{}
	err := putKV(store, 2, "two+1", startTime+3, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 2, "two+2", startTime+4, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 2, "two+3", startTime+5, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 2, "two+4", startTime+6, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 2, "two+5", startTime+7, iSerde, sSerde)
	if err != nil {
		return err
	}
	err = putKV(store, 2, "two+6", startTime+8, iSerde, sSerde)
	return err
}

func assertGet(store *InMemoryBytesWindowStore, k uint32, expected_val string, startTime int64, keySerde commtypes.Serde, valSerde commtypes.Serde) error {
	key, err := keySerde.Encode(k)
	if err != nil {
		return fmt.Errorf("fail to encode key: %v\n", err)
	}
	v, ok := store.Get(key, startTime)
	if !ok {
		return fmt.Errorf("key %d should exists\n", 0)
	}
	val, err := valSerde.Decode(v)
	if err != nil {
		return fmt.Errorf("fail to decode val: %v\n", err)
	}
	if val != expected_val {
		return fmt.Errorf("should be %s, but got %s", expected_val, val)
	}
	return nil
}

func assertFetch(store *InMemoryBytesWindowStore, k uint32, timeFrom int64, timeTo int64, keySerde commtypes.Serde, valSerde commtypes.Serde) (map[string]struct{}, error) {
	key, err := keySerde.Encode(k)
	if err != nil {
		return nil, fmt.Errorf("fail to encode key: %v\n", err)
	}
	res := make(map[string]struct{})
	err = store.Fetch(key, time.UnixMilli(timeFrom), time.UnixMilli(timeTo), func(i int64, vt ValueT) error {
		vtTmp := vt.(commtypes.ValueTimestamp)
		valBytes := vtTmp.Value.([]byte)
		val, err := valSerde.Decode(valBytes)
		if err != nil {
			return err
		}
		res[val.(string)] = struct{}{}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("fail to fetch: %v", err)
	}
	return res, nil
}

func TestGetAndRange(t *testing.T) {
	startTime := SEGMENT_INTERVAL - 4
	store := getWindowStore()
	err := putFirstBatch(store, startTime)
	if err != nil {
		t.Errorf("fail to set up window store: %v\n", err)
	}
	iSerde := commtypes.Uint32Serde{}
	sSerde := commtypes.StringSerde{}

	k := uint32(0)
	expected_val := "zero"
	key, err := iSerde.Encode(k)
	if err != nil {
		t.Errorf("fail to encode key: %v\n", err)
	}
	v, ok := store.Get(key, startTime)
	if !ok {
		t.Errorf("key %d should exists\n", 0)
	}
	val, err := sSerde.Decode(v)
	if err != nil {
		t.Errorf("fail to decode val: %v\n", err)
	}
	if val != expected_val {
		t.Errorf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(1)
	expected_val = "one"
	key, err = iSerde.Encode(k)
	if err != nil {
		t.Errorf("fail to encode key: %v\n", err)
	}
	v, ok = store.Get(key, startTime+1)
	if !ok {
		t.Errorf("key %d should exists\n", 0)
	}
	val, err = sSerde.Decode(v)
	if err != nil {
		t.Errorf("fail to decode val: %v\n", err)
	}
	if val != expected_val {
		t.Errorf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(2)
	expected_val = "two"
	key, err = iSerde.Encode(k)
	if err != nil {
		t.Errorf("fail to encode key: %v\n", err)
	}
	v, ok = store.Get(key, startTime+2)
	if !ok {
		t.Errorf("key %d should exists\n", 0)
	}
	val, err = sSerde.Decode(v)
	if err != nil {
		t.Errorf("fail to decode val: %v\n", err)
	}
	if val != expected_val {
		t.Errorf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(4)
	expected_val = "four"
	key, err = iSerde.Encode(k)
	if err != nil {
		t.Errorf("fail to encode key: %v\n", err)
	}
	v, ok = store.Get(key, startTime+4)
	if !ok {
		t.Errorf("key %d should exists\n", 0)
	}
	val, err = sSerde.Decode(v)
	if err != nil {
		t.Errorf("fail to decode val: %v\n", err)
	}
	if val != expected_val {
		t.Errorf("should be %s, but got %s", expected_val, val)
	}

	k = uint32(5)
	expected_val = "five"
	key, err = iSerde.Encode(k)
	if err != nil {
		t.Errorf("fail to encode key: %v\n", err)
	}
	v, ok = store.Get(key, startTime+5)
	if !ok {
		t.Errorf("key %d should exists\n", 0)
	}
	val, err = sSerde.Decode(v)
	if err != nil {
		t.Errorf("fail to decode val: %v\n", err)
	}
	if val != expected_val {
		t.Errorf("should be %s, but got %s", expected_val, val)
	}

	resM, err := assertFetch(store, 0, startTime-WINDOW_SIZE, startTime+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Errorf("fail to fetch: %v", err)
	}
	expected := "zero"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}

	k = uint32(0)
	expected_val = "zero"
	key, err = iSerde.Encode(k)
	if err != nil {
		t.Errorf("fail to encode key: %v\n", err)
	}
	var res []string
	err = store.Fetch(key, time.UnixMilli(startTime+0-WINDOW_SIZE), time.UnixMilli(startTime+0+WINDOW_SIZE), func(i int64, vt ValueT) error {
		vtTmp := vt.(commtypes.ValueTimestamp)
		valBytes := vtTmp.Value.([]byte)
		val, err = sSerde.Decode(valBytes)
		if err != nil {
			return err
		}
		res = append(res, val.(string))
		return nil
	})
	if err != nil {
		t.Errorf("fail to fetch: %v", err)
	}

	err = putSecondBatch(store, startTime)
	if err != nil {
		t.Errorf("fail to put second batch")
	}

	k = uint32(2)
	expected_val = "two+1"
	key, err = iSerde.Encode(k)
	if err != nil {
		t.Errorf("fail to encode key: %v\n", err)
	}
	v, ok = store.Get(key, startTime+3)
	if !ok {
		t.Errorf("key %d should exists\n", 0)
	}
	val, err = sSerde.Decode(v)
	if err != nil {
		t.Errorf("fail to decode val: %v\n", err)
	}
	if val != expected_val {
		t.Errorf("should be %s, but got %s", expected_val, val)
	}

	err = assertGet(store, 2, "two+2", startTime+4, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}

	err = assertGet(store, 2, "two+3", startTime+5, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}

	err = assertGet(store, 2, "two+4", startTime+6, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}

	err = assertGet(store, 2, "two+5", startTime+7, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}

	err = assertGet(store, 2, "two+6", startTime+8, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}

	resM, err = assertFetch(store, 2, startTime-2-WINDOW_SIZE, startTime+2+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	if len(res) != 0 {
		t.Errorf("expected empty list but got %v", resM)
	}

	resM, err = assertFetch(store, 2, startTime-1-WINDOW_SIZE, startTime+1+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	if _, ok := resM["two"]; !ok {
		t.Errorf("expected list contains two but got %v", resM)
	}

	resM, err = assertFetch(store, 2, startTime-WINDOW_SIZE, startTime+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+1-WINDOW_SIZE, startTime+1+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+2-WINDOW_SIZE, startTime+2+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+3-WINDOW_SIZE, startTime+3+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+4-WINDOW_SIZE, startTime+4+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}

	resM, err = assertFetch(store, 2, startTime+5-WINDOW_SIZE, startTime+5+WINDOW_SIZE, iSerde, sSerde)
	if err != nil {
		t.Error(err.Error())
	}
	expected = "two"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+1"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+2"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+3"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+4"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+5"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}
	expected = "two+6"
	if _, ok := resM[expected]; !ok {
		t.Errorf("expected list contains %s but got %v", expected, resM)
	}

}
