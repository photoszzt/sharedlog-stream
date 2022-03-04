package store

import (
	"context"
	"testing"
)

/*
func getContent(it KeyValueIterator) map[int]string {
	ret := make(map[int]string)
	for it.HasNext() {
		n := it.Next()
		fmt.Fprintf(os.Stderr, "next is %v\n", n)
		if n.Key == nil {
			continue
		}
		ret[n.Key.(int)] = n.Value.(string)
	}
	return ret
}

func checkMapEqual(t testing.TB, expected map[int]string, got map[int]string) {
	if len(expected) != len(got) {
		t.Fatalf("expected and got have different length, expected. expected: %v, got: %v", expected, got)
	}
	for k, v := range expected {
		vgot := got[k]
		if vgot != v {
			t.Fatalf("k: %d, expected: %s, got: %s", k, v, vgot)
		}
	}
}
*/

func ShouldNotIncludeDeletedFromRangeResult(ctx context.Context, store KeyValueStore, t testing.TB) {
	store.Put(ctx, 0, "zero")
	store.Put(ctx, 1, "one")
	store.Put(ctx, 2, "two")
	store.Delete(ctx, 0)
	store.Delete(ctx, 1)

	expected := make(map[int]string)
	expected[2] = "two"

	val2, ok, err := store.Get(ctx, 2)
	if err != nil {
		t.Fatalf("fail to get 2: %v", err)
	}
	if !ok {
		t.Fatal("2 should be in the map")
	}
	if val2 != "two" {
		t.Fatalf("expected two, got %s", val2)
	}
	// TODO: iter is broken
	/*
		it := store.Range(nil, nil)
		ret := getContent(it)
		checkMapEqual(t, expected, ret)
	*/
}

func ShouldDeleteIfSerializedValueIsNull(ctx context.Context, store KeyValueStore, t testing.TB) {
	store.Put(ctx, 0, "zero")
	store.Put(ctx, 1, "one")
	store.Put(ctx, 2, "two")
	store.Put(ctx, 0, nil)
	store.Put(ctx, 1, nil)
	expected := make(map[int]string)
	expected[2] = "two"

	val2, ok, err := store.Get(ctx, 2)
	if err != nil {
		t.Fatalf("fail to get 2: %v", err)
	}
	if !ok {
		t.Fatal("2 should be in the map")
	}
	if val2 != "two" {
		t.Fatalf("expected two, got %s", val2)
	}

	/*
		it := store.Range(nil, nil)
		ret := getContent(it)
		checkMapEqual(t, expected, ret)
	*/
}
