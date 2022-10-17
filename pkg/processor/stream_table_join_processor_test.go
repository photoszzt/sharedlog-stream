package processor

import (
	"context"
	"fmt"
	"reflect"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/optional"
	"sharedlog-stream/pkg/store"
	"testing"
)

// func getJoinProcessor() *StreamTableJoinProcessor {
// 	store := store.NewInMemoryKeyValueStore("test1", store.IntLess)
// 	joinProc := NewStreamTableJoinProcessor(store, ValueJoinerWithKeyFunc(
// 		func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
// 			lv := leftValue.(int)
// 			if rightValue != nil {
// 				rv := rightValue.(int)
// 				return fmt.Sprintf("%d+%d", lv, rv)
// 			} else {
// 				return nil
// 			}
// 		},
// 	))
// 	return joinProc
// }

func getSkipMapJoinProcessor() *StreamTableJoinProcessorG[int, int, int, string] {
	store := store.NewInMemorySkipmapKeyValueStoreG[int, commtypes.ValueTimestampG[int]]("test1", store.IntLessFunc)
	joinProc := NewStreamTableJoinProcessorG[int, int, int, string](store, ValueJoinerWithKeyFuncG[int, int, int, string](
		func(readOnlyKey int, leftValue int, rightValue int) string {
			return fmt.Sprintf("%d+%d", leftValue, rightValue)
		},
	))
	return joinProc
}

// func getJoinProcessorWithStr() *StreamTableJoinProcessor {
// 	store := store.NewInMemoryKeyValueStore("test1", store.IntLess)
// 	joinProc := NewStreamTableJoinProcessor(store, ValueJoinerWithKeyFunc(
// 		func(readOnlyKey interface{}, leftValue interface{}, rightValue interface{}) interface{} {
// 			lv := leftValue.(string)
// 			if rightValue != nil {
// 				rv := rightValue.(string)
// 				return fmt.Sprintf("%s+%s", lv, rv)
// 			} else {
// 				return nil
// 			}
// 		},
// 	))
// 	return joinProc
// }

func getSkipMapJoinProcessorWithStr() *StreamTableJoinProcessorG[int, string, string, string] {
	store := store.NewInMemorySkipmapKeyValueStoreG[int, commtypes.ValueTimestampG[string]]("test1", store.IntLessFunc)
	joinProc := NewStreamTableJoinProcessorG[int, string, string, string](store, ValueJoinerWithKeyFuncG[int, string, string, string](
		func(readOnlyKey int, leftValue string, rightValue string) string {
			return fmt.Sprintf("%s+%s", leftValue, rightValue)
		},
	))
	return joinProc
}

/*
func TestJoinOnlyIfMatchFound(t *testing.T) {
	ctx := context.Background()
	joinProc := getJoinProcessor()
	for i := 0; i < 2; i++ {
		err := joinProc.store.Put(ctx, i, commtypes.CreateValueTimestamp(i, int64(i)))
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
	JoinOnlyIfMatchFound(t, ctx, joinProc)
}
*/

func TestSkipMapJoinOnlyIfMatchFound(t *testing.T) {
	ctx := context.Background()
	joinProc := getSkipMapJoinProcessor()
	for i := 0; i < 2; i++ {
		err := joinProc.store.Put(ctx, i, commtypes.CreateValueTimestampGOptional(optional.Some(i), int64(i)), store.TimeMeta{RecordTsMs: int64(i)})
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
	JoinOnlyIfMatchFoundG(t, ctx, joinProc)
}

func JoinOnlyIfMatchFound(t *testing.T, ctx context.Context, joinProc Processor) {
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

func JoinOnlyIfMatchFoundG(t *testing.T, ctx context.Context, joinProc ProcessorG[int, int, int, string]) {
	for i := 0; i < 4; i++ {
		msgs, err := joinProc.ProcessAndReturn(ctx, commtypes.MessageG[int, int]{Key: optional.Some(i), Value: optional.Some(i), TimestampMs: int64(i)})
		if err != nil {
			t.Errorf("fail to join: %v", err)
		}
		if i == 0 || i == 1 {
			expected_join_val := fmt.Sprintf("%d+%d", i, i)
			if msgs[0].Key.Unwrap() != i && msgs[0].Value.Unwrap() != expected_join_val {
				t.Errorf("expected join val: %s, got %s", expected_join_val, msgs[0].Value.Unwrap())
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

/*
func TestShouldClearTableEntryOnNullValueUpdate(t *testing.T) {
	joinProc := getJoinProcessorWithStr()
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		err := joinProc.store.Put(ctx, i, commtypes.CreateValueTimestamp(fmt.Sprintf("Y%d", i), int64(i)))
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
	ShouldClearTableEntryOnNullValueUpdatePart1(t, ctx, joinProc)
	putSecond(t, ctx, joinProc)
	ShouldClearTableEntryOnNullValueUpdatePart2(t, ctx, joinProc)
}
*/

func TestSkipMapShouldClearTableEntryOnNullValueUpdate(t *testing.T) {
	joinProc := getSkipMapJoinProcessorWithStr()
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		err := joinProc.store.Put(ctx, i,
			commtypes.CreateValueTimestampGOptional(optional.Some(fmt.Sprintf("Y%d", i)), int64(i)),
			store.TimeMeta{RecordTsMs: int64(i)})
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
	ShouldClearTableEntryOnNullValueUpdatePart1G(t, ctx, joinProc)
	putSecondG(t, ctx, joinProc)
	ShouldClearTableEntryOnNullValueUpdatePart2G(t, ctx, joinProc)
}

/*
func putSecond(t *testing.T, ctx context.Context, joinProc *StreamTableJoinProcessor) {
	for i := 0; i < 2; i++ {
		err := joinProc.store.Put(ctx, i, nil)
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
}
*/

func putSecondG(t *testing.T, ctx context.Context, joinProc *StreamTableJoinProcessorG[int, string, string, string]) {
	for i := 0; i < 2; i++ {
		err := joinProc.store.Put(ctx, i, optional.None[commtypes.ValueTimestampG[string]](), store.TimeMeta{RecordTsMs: 0})
		if err != nil {
			t.Errorf("fail to put val to store: %v", err)
		}
	}
}

func ShouldClearTableEntryOnNullValueUpdatePart1(t *testing.T, ctx context.Context, joinProc Processor) {
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
}

func ShouldClearTableEntryOnNullValueUpdatePart1G(t *testing.T, ctx context.Context, joinProc ProcessorG[int, string, int, string]) {
	got_msgs := make([]commtypes.MessageG[int, string], 0)
	for i := 0; i < 4; i++ {
		msgs, err := joinProc.ProcessAndReturn(ctx,
			commtypes.MessageG[int, string]{Key: optional.Some(i), Value: optional.Some(fmt.Sprintf("X%d", i)), TimestampMs: int64(i)})
		if err != nil {
			t.Errorf("fail to join: %v", err)
		}
		got_msgs = append(got_msgs, msgs...)
	}
	expected_msgs := []commtypes.MessageG[int, string]{
		{Key: optional.Some(0), Value: optional.Some("X0+Y0"), TimestampMs: 0},
		{Key: optional.Some(1), Value: optional.Some("X1+Y1"), TimestampMs: 1},
		{Key: optional.Some(2), Value: optional.Some("X2+Y2"), TimestampMs: 2},
		{Key: optional.Some(3), Value: optional.Some("X3+Y3"), TimestampMs: 3},
	}
	if !reflect.DeepEqual(expected_msgs, got_msgs) {
		t.Fatal("should equal")
	}
}

func ShouldClearTableEntryOnNullValueUpdatePart2(t *testing.T, ctx context.Context, joinProc Processor) {
	got_msgs := make([]commtypes.Message, 0)
	for i := 0; i < 4; i++ {
		msgs, err := joinProc.ProcessAndReturn(ctx,
			commtypes.Message{Key: i, Value: fmt.Sprintf("XX%d", i), Timestamp: int64(i)})
		if err != nil {
			t.Errorf("fail to join: %v", err)
		}
		got_msgs = append(got_msgs, msgs...)
	}
	expected_msgs := []commtypes.Message{
		{Key: optional.Some(2), Value: optional.Some("XX2+Y2"), Timestamp: 2},
		{Key: optional.Some(3), Value: optional.Some("XX3+Y3"), Timestamp: 3},
	}
	if !reflect.DeepEqual(expected_msgs, got_msgs) {
		t.Fatal("should equal")
	}
}

func ShouldClearTableEntryOnNullValueUpdatePart2G(t *testing.T, ctx context.Context, joinProc ProcessorG[int, string, int, string]) {
	got_msgs := make([]commtypes.MessageG[int, string], 0)
	for i := 0; i < 4; i++ {
		msgs, err := joinProc.ProcessAndReturn(ctx,
			commtypes.MessageG[int, string]{Key: optional.Some(i), Value: optional.Some(fmt.Sprintf("XX%d", i)), TimestampMs: int64(i)})
		if err != nil {
			t.Errorf("fail to join: %v", err)
		}
		got_msgs = append(got_msgs, msgs...)
	}
	expected_msgs := []commtypes.Message{
		{Key: optional.Some(2), Value: optional.Some("XX2+Y2"), Timestamp: 2},
		{Key: optional.Some(3), Value: optional.Some("XX3+Y3"), Timestamp: 3},
	}
	if !reflect.DeepEqual(expected_msgs, got_msgs) {
		t.Fatal("should equal")
	}
}
