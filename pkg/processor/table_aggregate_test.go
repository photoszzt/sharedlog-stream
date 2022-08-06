package processor

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"strings"
	"testing"
)

func toStringAgg(sep string) AggregatorFunc {
	return AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
		agg := aggregate.(string)
		val := value.(string)
		return agg + sep + val
	})
}

func toStringAggG(sep string) AggregatorFuncG[string, string, string] {
	return AggregatorFuncG[string, string, string](func(key string, value string, aggregate string) string {
		return aggregate + sep + value
	})
}

var (
	TOSTRING_ADDER     = toStringAgg("+")
	TOSTRING_REMOVER   = toStringAgg("-")
	TOSTRING_ADDER_G   = toStringAggG("+")
	TOSTRING_REMOVER_G = toStringAggG("-")
	STRING_INIT        = InitializerFunc(func() interface{} {
		return "0"
	})
	STRING_INIT_G = InitializerFuncG[string](func() string {
		return "0"
	})
)

func TestAggBasicWithInMemKVTable(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemoryKeyValueStore("srcTab", store.StringLess)
	st := store.NewInMemoryKeyValueStore("aggTab", store.StringLess)
	pc.
		Via(NewTableSourceProcessorWithTable(srcTable)).
		Via(NewTableGroupByMapProcessor("noOpGroupBy",
			MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
				return key, value, nil
			}))).
		Via(NewTableAggregateProcessor("agg", st, STRING_INIT, TOSTRING_ADDER, TOSTRING_REMOVER)).
		Via(NewTableToStreamProcessor())
	testAggBasic(t, &pc)
}

func TestAggBasicWithInMemSkipmapTable(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("srcTab", store.StringLessFunc)
	st := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("aggTab", store.StringLessFunc)
	pc.
		Via(NewTableSourceProcessorWithTableG[string, string](srcTable)).
		Via(NewTableGroupByMapProcessor("noOpGroupBy",
			MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
				return key, value, nil
			}))).
		Via(NewTableAggregateProcessorG[string, string, string]("agg", st, STRING_INIT_G, TOSTRING_ADDER_G, TOSTRING_REMOVER_G)).
		Via(NewTableToStreamProcessor())
	testAggBasic(t, &pc)
}

func testAggBasic(t *testing.T, pc *ProcessorChains) {
	ctx := context.Background()
	inputMsgs := []commtypes.Message{
		{Key: "A", Value: "1", Timestamp: 10},
		{Key: "B", Value: "2", Timestamp: 15},
		{Key: "A", Value: "3", Timestamp: 20},
		{Key: "B", Value: "4", Timestamp: 18},
		{Key: "C", Value: "5", Timestamp: 5},
		{Key: "D", Value: "6", Timestamp: 25},
		{Key: "B", Value: "7", Timestamp: 15},
		{Key: "C", Value: "8", Timestamp: 10},
	}
	var outMsgs []commtypes.Message
	for _, inMsg := range inputMsgs {
		out, err := pc.RunChains(ctx, inMsg)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		outMsgs = append(outMsgs, out...)
	}
	expected_out := []commtypes.Message{
		{Key: "A", Value: "0+1", Timestamp: 10},
		{Key: "B", Value: "0+2", Timestamp: 15},
		{Key: "A", Value: "0+1-1", Timestamp: 20},
		{Key: "A", Value: "0+1-1+3", Timestamp: 20},
		{Key: "B", Value: "0+2-2", Timestamp: 18},
		{Key: "B", Value: "0+2-2+4", Timestamp: 18},
		{Key: "C", Value: "0+5", Timestamp: 5},
		{Key: "D", Value: "0+6", Timestamp: 25},
		{Key: "B", Value: "0+2-2+4-4", Timestamp: 18},
		{Key: "B", Value: "0+2-2+4-4+7", Timestamp: 18},
		{Key: "C", Value: "0+5-5", Timestamp: 10},
		{Key: "C", Value: "0+5-5+8", Timestamp: 10},
	}
	if !reflect.DeepEqual(expected_out, outMsgs) {
		fmt.Fprintf(os.Stderr, "Expected output: \n")
		for _, expected := range expected_out {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %s, ts %d\n", expected.Key, expected.Value, expected.Timestamp)
		}
		fmt.Fprintf(os.Stderr, "Got output: \n")
		for _, outMsg := range outMsgs {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %s, ts %d\n", outMsg.Key, outMsg.Value, outMsg.Timestamp)
		}
		t.Fatalf("should equal.")
	}
}

func TestAggRepartitionWithInMemKVStore(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemoryKeyValueStore("srcTab", store.StringLess)
	st := store.NewInMemoryKeyValueStore("aggTab", store.StringLess)
	pc.
		Via(NewTableSourceProcessorWithTable(srcTable)).
		Via(NewTableGroupByMapProcessor("groupBy", MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
			k := key.(string)
			if k == "null" {
				return nil, value, nil
			} else if k == "NULL" {
				return nil, nil, nil
			} else {
				return value, value, nil
			}
		}))).
		Via(NewTableAggregateProcessor("agg", st, STRING_INIT, TOSTRING_ADDER, TOSTRING_REMOVER)).
		Via(NewTableToStreamProcessor())
	testAggRepartition(t, &pc)
}

func TestAggRepartitionWithInMemSkipmapKVStore(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("srcTab", store.StringLessFunc)
	st := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("aggTab", store.StringLessFunc)
	pc.
		Via(NewTableSourceProcessorWithTableG[string, string](srcTable)).
		Via(NewTableGroupByMapProcessor("groupBy", MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
			k := key.(string)
			if k == "null" {
				return nil, value, nil
			} else if k == "NULL" {
				return nil, nil, nil
			} else {
				return value, value, nil
			}
		}))).
		Via(NewTableAggregateProcessorG[string, string, string]("agg", st, STRING_INIT_G, TOSTRING_ADDER_G, TOSTRING_REMOVER_G)).
		Via(NewTableToStreamProcessor())
	testAggRepartition(t, &pc)
}

func testAggRepartition(t *testing.T, pc *ProcessorChains) {
	ctx := context.Background()
	inputMsgs := []commtypes.Message{
		{Key: "A", Value: "1", Timestamp: 10},
		{Key: "A", Value: nil, Timestamp: 15},
		{Key: "A", Value: "1", Timestamp: 12},
		{Key: "B", Value: "2", Timestamp: 20},
		{Key: "null", Value: "3", Timestamp: 25},
		{Key: "B", Value: "4", Timestamp: 23},
		{Key: "NULL", Value: "5", Timestamp: 24},
		{Key: "B", Value: "7", Timestamp: 22},
	}
	var outMsgs []commtypes.Message
	for _, inMsg := range inputMsgs {
		out, err := pc.RunChains(ctx, inMsg)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		outMsgs = append(outMsgs, out...)
	}
	expected_out := []commtypes.Message{
		{Key: "1", Value: "0+1", Timestamp: 10},
		{Key: "1", Value: "0+1-1", Timestamp: 15},
		{Key: "1", Value: "0+1-1+1", Timestamp: 15},
		{Key: "2", Value: "0+2", Timestamp: 20},
		{Key: "2", Value: "0+2-2", Timestamp: 23},
		{Key: "4", Value: "0+4", Timestamp: 23},
		{Key: "4", Value: "0+4-4", Timestamp: 23},
		{Key: "7", Value: "0+7", Timestamp: 22},
	}
	if !reflect.DeepEqual(expected_out, outMsgs) {
		fmt.Fprintf(os.Stderr, "Expected output: \n")
		for _, expected := range expected_out {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %s, ts %d\n", expected.Key, expected.Value, expected.Timestamp)
		}
		fmt.Fprintf(os.Stderr, "Got output: \n")
		for _, outMsg := range outMsgs {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %v, ts %d\n", outMsg.Key, outMsg.Value, outMsg.Timestamp)
		}
		t.Fatalf("should equal.")
	}
}

func TestCountWithInMemKVStore(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemoryKeyValueStore("srcTab", store.StringLess)
	st := store.NewInMemoryKeyValueStore("aggTab", store.StringLess)
	pc.
		Via(NewTableSourceProcessorWithTable(srcTable)).
		Via(NewTableGroupByMapProcessor("groupBy", MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
			return value, value, nil
		}))).
		Via(NewTableAggregateProcessor("count", st,
			InitializerFunc(func() interface{} { return uint64(0) }),
			AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
				agg := aggregate.(uint64)
				return agg + 1
			}),
			AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
				agg := aggregate.(uint64)
				return agg - 1
			}))).
		Via(NewTableToStreamProcessor())
	testCount(t, &pc)
}

func TestCountWithInMemSkipmapKVStore(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("srcTab", store.StringLessFunc)
	st := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("aggTab", store.StringLessFunc)
	pc.
		Via(NewTableSourceProcessorWithTableG[string, string](srcTable)).
		Via(NewTableGroupByMapProcessor("groupBy", MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
			return value, value, nil
		}))).
		Via(NewTableAggregateProcessorG[string, string, uint64]("count", st,
			InitializerFuncG[uint64](func() uint64 { return uint64(0) }),
			AggregatorFuncG[string, string, uint64](func(key string, value string, aggregate uint64) uint64 {
				return aggregate + 1
			}),
			AggregatorFuncG[string, string, uint64](func(key string, value string, aggregate uint64) uint64 {
				return aggregate - 1
			}))).
		Via(NewTableToStreamProcessor())
	testCount(t, &pc)
}

func testCount(t *testing.T, pc *ProcessorChains) {
	inputMsgs := []commtypes.Message{
		{Key: "A", Value: "green", Timestamp: 10},
		{Key: "B", Value: "green", Timestamp: 9},
		{Key: "A", Value: "blue", Timestamp: 12},
		{Key: "C", Value: "yellow", Timestamp: 15},
		{Key: "D", Value: "green", Timestamp: 11},
	}
	ctx := context.Background()
	var outMsgs []commtypes.Message
	for _, inMsg := range inputMsgs {
		out, err := pc.RunChains(ctx, inMsg)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		outMsgs = append(outMsgs, out...)
	}
	expected_out := []commtypes.Message{
		{Key: "green", Value: uint64(1), Timestamp: 10},
		{Key: "green", Value: uint64(2), Timestamp: 10},
		{Key: "green", Value: uint64(1), Timestamp: 12},
		{Key: "blue", Value: uint64(1), Timestamp: 12},
		{Key: "yellow", Value: uint64(1), Timestamp: 15},
		{Key: "green", Value: uint64(2), Timestamp: 12},
	}
	if !reflect.DeepEqual(expected_out, outMsgs) {
		fmt.Fprintf(os.Stderr, "Expected output: \n")
		for _, expected := range expected_out {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %d, ts %d\n", expected.Key, expected.Value, expected.Timestamp)
		}
		fmt.Fprintf(os.Stderr, "Got output: \n")
		for _, outMsg := range outMsgs {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %#+v, ts %#+v\n", outMsg.Key, outMsg.Value, outMsg.Timestamp)
		}
		t.Fatalf("should equal.")
	}
}

func TestRemoveOldBeforeAddNewWithInMemKVStore(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemoryKeyValueStore("srcTab", store.StringLess)
	st := store.NewInMemoryKeyValueStore("aggTab", store.StringLess)
	pc.
		Via(NewTableSourceProcessorWithTable(srcTable)).
		Via(NewTableGroupByMapProcessor("groupBy", MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
			k := key.(string)
			return string(k[0]), string(k[1]), nil
		}))).
		Via(NewTableAggregateProcessor("agg", st,
			InitializerFunc(func() interface{} { return "" }),
			AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
				agg := aggregate.(string)
				val := value.(string)
				return agg + val
			}),
			AggregatorFunc(func(key, value, aggregate interface{}) interface{} {
				agg := aggregate.(string)
				val := value.(string)
				return strings.ReplaceAll(agg, val, "")
			}))).
		Via(NewTableToStreamProcessor())
	testRemoveOldBeforeAddNew(t, &pc)
}

func TestRemoveOldBeforeAddNewWithInMemSkipmapKVStore(t *testing.T) {
	pc := NewProcessorChains()
	srcTable := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("srcTab", store.StringLessFunc)
	st := store.NewInMemorySkipmapKeyValueStoreG[string, *commtypes.ValueTimestamp]("aggTab", store.StringLessFunc)
	pc.
		Via(NewTableSourceProcessorWithTableG[string, string](srcTable)).
		Via(NewTableGroupByMapProcessor("groupBy", MapperFunc(func(key, value interface{}) (interface{}, interface{}, error) {
			k := key.(string)
			return string(k[0]), string(k[1]), nil
		}))).
		Via(NewTableAggregateProcessorG[string, string, string]("agg", st,
			InitializerFuncG[string](func() string { return "" }),
			AggregatorFuncG[string, string, string](func(key string, value string, aggregate string) string {
				return aggregate + value
			}),
			AggregatorFuncG[string, string, string](func(key string, value string, aggregate string) string {
				return strings.ReplaceAll(aggregate, value, "")
			}))).
		Via(NewTableToStreamProcessor())
	testRemoveOldBeforeAddNew(t, &pc)
}

func testRemoveOldBeforeAddNew(t *testing.T, pc *ProcessorChains) {
	inputMsgs := []commtypes.Message{
		{Key: "11", Value: "A", Timestamp: 10},
		{Key: "12", Value: "B", Timestamp: 8},
		{Key: "11", Value: nil, Timestamp: 12},
		{Key: "12", Value: "C", Timestamp: 6},
	}
	ctx := context.Background()
	var outMsgs []commtypes.Message
	for _, inMsg := range inputMsgs {
		out, err := pc.RunChains(ctx, inMsg)
		if err != nil {
			t.Fatalf("err: %v", err)
		}
		outMsgs = append(outMsgs, out...)
	}
	expected_out := []commtypes.Message{
		{Key: "1", Value: "1", Timestamp: 10},
		{Key: "1", Value: "12", Timestamp: 10},
		{Key: "1", Value: "2", Timestamp: 12},
		{Key: "1", Value: "", Timestamp: 12},
		{Key: "1", Value: "2", Timestamp: 12},
	}
	if !reflect.DeepEqual(expected_out, outMsgs) {
		fmt.Fprintf(os.Stderr, "Expected output: \n")
		for _, expected := range expected_out {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %s, ts %d\n", expected.Key, expected.Value, expected.Timestamp)
		}
		fmt.Fprintf(os.Stderr, "Got output: \n")
		for _, outMsg := range outMsgs {
			fmt.Fprintf(os.Stderr, "\tgot k %s, val %s, ts %d\n", outMsg.Key, outMsg.Value, outMsg.Timestamp)
		}
		t.Fatalf("should equal.")
	}
}
