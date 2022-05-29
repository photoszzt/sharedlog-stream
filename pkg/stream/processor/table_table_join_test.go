package processor

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/treemap"
	"testing"
)

func getJoinTable(t *testing.T) (
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
) {
	compare := func(a, b treemap.Key) int {
		valA := a.(int)
		valB := b.(int)
		if valA < valB {
			return -1
		} else if valA == valB {
			return 0
		} else {
			return 1
		}
	}
	toTab1, tab1 := ToInMemKVTable("tab1", compare, 0)
	toTab2, tab2 := ToInMemKVTable("tab2", compare, 0)
	joiner := ValueJoinerWithKeyFunc(
		func(readOnlyKey interface{},
			leftValue interface{}, rightValue interface{},
		) interface{} {
			debug.Fprintf(os.Stderr, "left val: %v, right val: %v\n", leftValue, rightValue)
			lstr, ok := leftValue.(string)
			if !ok {
				lv := leftValue.(commtypes.ValueTimestamp)
				lstr = lv.Value.(string)
			}
			rstr, ok := rightValue.(string)
			if !ok {
				rv := rightValue.(commtypes.ValueTimestamp)
				rstr = rv.Value.(string)
			}
			return fmt.Sprintf("%s+%s", lstr, rstr)
		})
	oneJoinTwo := NewTableTableJoinProcessor(tab2.Name(), tab2, joiner)
	twoJoinOne := NewTableTableJoinProcessor(tab1.Name(), tab1, ReverseValueJoinerWithKey(joiner))
	oneJoinTwoFunc := func(ctx context.Context, m commtypes.Message) []commtypes.Message {
		_, err := toTab1.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		joinedMsgs, err := oneJoinTwo.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		return joinedMsgs
	}
	twoJoinOneFunc := func(ctx context.Context, m commtypes.Message) []commtypes.Message {
		_, err := toTab2.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		joinedMsgs, err := twoJoinOne.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		return joinedMsgs
	}
	return oneJoinTwoFunc, twoJoinOneFunc
}

func TestTableTableInnerJoin(t *testing.T) {
	ctx := context.Background()
	oneJoinTwo, twoJoinOne := getJoinTable(t)
	expected_keys := []int{0, 1, 2, 3}
	got := make([]commtypes.Message, 0)
	for i := 0; i < 2; i++ {
		ret := oneJoinTwo(ctx, commtypes.Message{Key: expected_keys[i],
			Value: fmt.Sprintf("X%d", expected_keys[i]), Timestamp: 5 + int64(i)})
		got = append(got, ret...)
	}
	ret := oneJoinTwo(ctx, commtypes.Message{Key: nil,
		Value: "SomeVal", Timestamp: 42})
	// left: X0:0 (ts: 5), X1:1 (ts: 6)
	// right:
	got = append(got, ret...)
	if len(got) != 0 {
		t.Fatal("should be empty")
	}

	// push two items to the other stream. this should produce two items.
	for i := 0; i < 2; i++ {
		ret := twoJoinOne(ctx, commtypes.Message{Key: expected_keys[i],
			Value: fmt.Sprintf("Y%d", expected_keys[i]), Timestamp: 10 * int64(i)})
		got = append(got, ret...)
	}
	ret = twoJoinOne(ctx, commtypes.Message{Key: nil,
		Value: "AnotherVal", Timestamp: 73})
	got = append(got, ret...)
	// left: X0:0 (ts: 5), X1:1 (ts: 6)
	// right: Y0:0 (ts: 0), Y1:1 (ts: 10)
	expected_join := []commtypes.Message{
		{Key: 0, Value: "X0+Y0", Timestamp: 5},
		{Key: 1, Value: "X1+Y1", Timestamp: 10},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	got = make([]commtypes.Message, 0)
	for _, k := range expected_keys {
		ret := oneJoinTwo(ctx, commtypes.Message{Key: k,
			Value: fmt.Sprintf("XX%d", k), Timestamp: 7})
		got = append(got, ret...)
	}
	// left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
	// right: Y0:0 (ts: 0), Y1:1 (ts: 10)
	expected_join = []commtypes.Message{
		{Key: 0, Value: "XX0+Y0", Timestamp: 7},
		{Key: 1, Value: "XX1+Y1", Timestamp: 10},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	got = make([]commtypes.Message, 0)
	for _, k := range expected_keys {
		ret := twoJoinOne(ctx, commtypes.Message{Key: k,
			Value: fmt.Sprintf("YY%d", k), Timestamp: int64(k * 5)})
		got = append(got, ret...)
	}
	// left: XX0:0 (ts: 7), XX1:1 (ts: 7), XX2:2 (ts: 7), XX3:3 (ts: 7)
	// right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
	expected_join = []commtypes.Message{
		{Key: 0, Value: "XX0+YY0", Timestamp: 10},
		{Key: 1, Value: "XX1+YY1", Timestamp: 10},
		{Key: 2, Value: "XX2+YY2", Timestamp: 10},
		{Key: 3, Value: "XX3+YY3", Timestamp: 15},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	got = make([]commtypes.Message, 0)
	for _, k := range expected_keys {
		ret := oneJoinTwo(ctx, commtypes.Message{Key: k,
			Value: fmt.Sprintf("XXX%d", k), Timestamp: 6})
		got = append(got, ret...)
	}
	// left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
	// right: YY0:0 (ts: 0), YY1:1 (ts: 5), YY2:2 (ts: 10), YY3:3 (ts: 15)
	expected_join = []commtypes.Message{
		{Key: 0, Value: "XXX0+YY0", Timestamp: 7},
		{Key: 1, Value: "XXX1+YY1", Timestamp: 7},
		{Key: 2, Value: "XXX2+YY2", Timestamp: 10},
		{Key: 3, Value: "XXX3+YY3", Timestamp: 15},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	debug.Fprintf(os.Stderr, "####### removing YY0:0 and YY1:1")
	got = make([]commtypes.Message, 0)
	// push two items with null to the other stream as deletes. this should produce two item.
	ret = twoJoinOne(ctx, commtypes.Message{Key: expected_keys[0], Value: nil, Timestamp: 5})
	got = append(got, ret...)
	ret = twoJoinOne(ctx, commtypes.Message{Key: expected_keys[1], Value: nil, Timestamp: 7})
	got = append(got, ret...)
	// left: XXX0:0 (ts: 6), XXX1:1 (ts: 6), XXX2:2 (ts: 6), XXX3:3 (ts: 6)
	// right: YY2:2 (ts: 10), YY3:3 (ts: 15)
	expected_join = []commtypes.Message{
		{Key: 0, Value: nil, Timestamp: 15},
		{Key: 1, Value: nil, Timestamp: 15},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	// push all four items to the primary stream. this should produce two items.
	got = make([]commtypes.Message, 0)
	for _, k := range expected_keys {
		ret := oneJoinTwo(ctx, commtypes.Message{Key: k,
			Value: fmt.Sprintf("XXXX%d", k), Timestamp: 13})
		got = append(got, ret...)
	}
	// left: XXXX0:0 (ts: 13), XXXX1:1 (ts: 13), XXXX2:2 (ts: 13), XXXX3:3 (ts: 13)
	// right: YY2:2 (ts: 10), YY3:3 (ts: 15)
	expected_join = []commtypes.Message{
		{Key: 2, Value: "XXXX2+YY2", Timestamp: 13},
		{Key: 3, Value: "XXXX3+YY3", Timestamp: 15},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	got = make([]commtypes.Message, 0)
	// push four items to the primary stream with null. this should produce two items.
	ret = oneJoinTwo(ctx, commtypes.Message{Key: expected_keys[0], Value: nil, Timestamp: 0})
	got = append(got, ret...)
	ret = oneJoinTwo(ctx, commtypes.Message{Key: expected_keys[1], Value: nil, Timestamp: 42})
	got = append(got, ret...)
	ret = oneJoinTwo(ctx, commtypes.Message{Key: expected_keys[2], Value: nil, Timestamp: 5})
	got = append(got, ret...)
	ret = oneJoinTwo(ctx, commtypes.Message{Key: expected_keys[3], Value: nil, Timestamp: 20})
	got = append(got, ret...)
	// left:
	// right: YY2:2 (ts: 10), YY3:3 (ts: 15)
	expected_join = []commtypes.Message{
		{Key: 2, Value: nil, Timestamp: 13},
		{Key: 3, Value: nil, Timestamp: 20},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}
}
