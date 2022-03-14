package processor

import (
	"context"
	"fmt"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
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
