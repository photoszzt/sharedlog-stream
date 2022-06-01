package processor

import (
	"context"
	"fmt"
	"reflect"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/treemap"
	"testing"
)

func getJoinProcessor() *StreamTableJoinProcessor {
	store := store.NewInMemoryKeyValueStore("test1", func(a, b treemap.Key) int {
		vala := a.(int)
		valb := b.(int)
		if vala > valb {
			return 1
		} else if vala == valb {
			return 0
		} else {
			return -1
		}
	})
	joinProc := NewStreamTableJoinProcessor("test1", store, ValueJoinerWithKeyFunc(
		func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
			lv := leftValue.(int)
			if rightValue != nil {
				rv := rightValue.(commtypes.ValueTimestamp)
				return fmt.Sprintf("%d+%d", lv, rv.Value.(int))
			} else {
				return nil
			}
		},
	))
	return joinProc
}

func getJoinProcessorWithStr() *StreamTableJoinProcessor {
	store := store.NewInMemoryKeyValueStore("test1", func(a, b treemap.Key) int {
		vala := a.(int)
		valb := b.(int)
		if vala > valb {
			return 1
		} else if vala == valb {
			return 0
		} else {
			return -1
		}
	})
	joinProc := NewStreamTableJoinProcessor("test1", store, ValueJoinerWithKeyFunc(
		func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
			lv := leftValue.(string)
			if rightValue != nil {
				rv := rightValue.(commtypes.ValueTimestamp)
				return fmt.Sprintf("%s+%s", lv, rv.Value.(string))
			} else {
				return nil
			}
		},
	))
	return joinProc
}

func TestJoinOnlyIfMatchFound(t *testing.T) {
	joinProc := getJoinProcessor()
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		err := joinProc.store.Put(ctx, i, commtypes.ValueTimestamp{Value: i, Timestamp: int64(i)})
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
	for i := 0; i < 4; i++ {
		msgs, err := joinProc.ProcessAndReturn(ctx, commtypes.Message{Key: i, Value: i, Timestamp: int64(i)})
		if err != nil {
			t.Errorf("fail to join: %v", err)
		}
		if i == 0 || i == 1 {
			expected_join_val := fmt.Sprintf("%d+%d", i, i)
			if msgs[0].Key != i && msgs[0].Value != expected_join_val {
				t.Errorf("expected join val: %s, got %s", expected_join_val, msgs[0].Value)
			}
		}
		if i == 3 || i == 4 {
			// debug.Fprintf(os.Stderr, "msgs: %v", msgs)
			if len(msgs) != 0 {
				t.Error("should return no value")
			}
		}
	}
}

func TestShouldClearTableEntryOnNullValueUpdate(t *testing.T) {
	joinProc := getJoinProcessorWithStr()
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		err := joinProc.store.Put(ctx, i, commtypes.ValueTimestamp{Value: fmt.Sprintf("Y%d", i), Timestamp: int64(i)})
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
	got_msgs := make([]commtypes.Message, 0)
	for i := 0; i < 4; i++ {
		msgs, err := joinProc.ProcessAndReturn(ctx,
			commtypes.Message{Key: i, Value: fmt.Sprintf("X%d", i), Timestamp: int64(i)})
		if err != nil {
			t.Errorf("fail to join: %v", err)
		}
		got_msgs = append(got_msgs, msgs...)
	}
	expected_msgs := []commtypes.Message{
		{Key: 0, Value: "X0+Y0", Timestamp: 0},
		{Key: 1, Value: "X1+Y1", Timestamp: 1},
		{Key: 2, Value: "X2+Y2", Timestamp: 2},
		{Key: 3, Value: "X3+Y3", Timestamp: 3},
	}
	if !reflect.DeepEqual(expected_msgs, got_msgs) {
		t.Fatal("should equal")
	}

	for i := 0; i < 2; i++ {
		err := joinProc.store.Put(ctx, i, nil)
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}

	got_msgs = make([]commtypes.Message, 0)
	for i := 0; i < 4; i++ {
		msgs, err := joinProc.ProcessAndReturn(ctx,
			commtypes.Message{Key: i, Value: fmt.Sprintf("XX%d", i), Timestamp: int64(i)})
		if err != nil {
			t.Errorf("fail to join: %v", err)
		}
		got_msgs = append(got_msgs, msgs...)
	}
	expected_msgs = []commtypes.Message{
		{Key: 2, Value: "XX2+Y2", Timestamp: 2},
		{Key: 3, Value: "XX3+Y3", Timestamp: 3},
	}
	if !reflect.DeepEqual(expected_msgs, got_msgs) {
		t.Fatal("should equal")
	}
}
