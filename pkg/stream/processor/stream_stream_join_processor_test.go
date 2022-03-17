package processor

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"testing"
	"time"
)

func getStreamJoin(t *testing.T) (
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
) {
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	compare := concurrent_skiplist.CompareFunc(func(lhs, rhs interface{}) int {
		l, ok := lhs.(int)
		if ok {
			r := rhs.(int)
			if l < r {
				return -1
			} else if l == r {
				return 0
			} else {
				return 1
			}
		} else {
			lv := lhs.(store.VersionedKey)
			rv := rhs.(store.VersionedKey)
			lvk := lv.Key.(int)
			rvk := rv.Key.(int)
			if lvk < rvk {
				return -1
			} else if lvk == rvk {
				if lv.Version < rv.Version {
					return -1
				} else if lv.Version == rv.Version {
					return 0
				} else {
					return 1
				}
			} else {
				return 1
			}
		}
	})
	toWinTab1, winTab1, err := ToInMemWindowTable("tab1", joinWindows, compare)
	if err != nil {
		t.Fatal(err.Error())
	}
	toWinTab2, winTab2, err := ToInMemWindowTable("tab2", joinWindows, compare)
	if err != nil {
		t.Fatal(err.Error())
	}
	joiner := ValueJoinerWithKeyTsFunc(
		func(readOnlyKey interface{},
			leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64,
		) interface{} {
			debug.Fprintf(os.Stderr, "left val: %v, right val: %v\n", leftValue, rightValue)
			return fmt.Sprintf("%s+%s", leftValue.(string), rightValue.(string))
		})
	sharedTimeTracker := NewTimeTracker()
	oneJoinTwoProc := NewStreamStreamJoinProcessor(winTab2, joinWindows, joiner, false, true, sharedTimeTracker)
	twoJoinOneProc := NewStreamStreamJoinProcessor(winTab1, joinWindows, ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker)
	oneJoinTwo := func(ctx context.Context, m commtypes.Message) []commtypes.Message {
		_, err := toWinTab1.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		joinedMsgs, err := oneJoinTwoProc.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		return joinedMsgs
	}
	twoJoinOne := func(ctx context.Context, m commtypes.Message) []commtypes.Message {
		_, err := toWinTab2.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		joinedMsgs, err := twoJoinOneProc.ProcessAndReturn(ctx, m)
		if err != nil {
			t.Fatal(err.Error())
		}
		return joinedMsgs
	}
	return oneJoinTwo, twoJoinOne
}

func TestJoin(t *testing.T) {
	ctx := context.Background()
	oneJoinTwo, twoJoinOne := getStreamJoin(t)
	expected_keys := []int{0, 1, 2, 3}
	// push two items to the primary stream; the other window is empty
	// w1 = {}
	// w2 = {}
	// --> w1 = { 0:A0, 1:A1 }
	//     w2 = {}
	got := make([]commtypes.Message, 0)
	for i := 0; i < 2; i++ {
		ret := oneJoinTwo(ctx, commtypes.Message{Key: expected_keys[i], Value: fmt.Sprintf("A%d", expected_keys[i])})
		got = append(got, ret...)
	}
	if len(got) != 0 {
		t.Fatal("should produce no result")
	}

	got = make([]commtypes.Message, 0)
	// push two items to the other stream; this should produce two items
	// w1 = { 0:A0, 1:A1 }
	// w2 = {}
	// --> w1 = { 0:A0, 1:A1 }
	//     w2 = { 0:a0, 1:a1 }
	for i := 0; i < 2; i++ {
		ret := twoJoinOne(ctx, commtypes.Message{Key: expected_keys[i], Value: fmt.Sprintf("a%d", expected_keys[i])})
		got = append(got, ret...)
	}
	expected_join := []commtypes.Message{
		{Key: 0, Value: "A0+a0", Timestamp: 0},
		{Key: 1, Value: "A1+a1", Timestamp: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	got = make([]commtypes.Message, 0)
	// push all four items to the primary stream; this should produce two items
	// w1 = { 0:A0, 1:A1 }
	// w2 = { 0:a0, 1:a1 }
	// --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
	//     w2 = { 0:a0, 1:a1 }
	for _, k := range expected_keys {
		ret := oneJoinTwo(ctx, commtypes.Message{Key: k, Value: fmt.Sprintf("B%d", k)})
		got = append(got, ret...)
	}
	expected_join = []commtypes.Message{
		{Key: 0, Value: "B0+a0", Timestamp: 0},
		{Key: 1, Value: "B1+a1", Timestamp: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatal("should equal")
	}

	got = make([]commtypes.Message, 0)
	// push all items to the other stream; this should produce six items
	// w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
	// w2 = { 0:a0, 1:a1 }
	// --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
	//     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
	for _, k := range expected_keys {
		ret := twoJoinOne(ctx, commtypes.Message{Key: k, Value: fmt.Sprintf("b%d", k)})
		got = append(got, ret...)
	}
	expected_join = []commtypes.Message{
		{Key: 0, Value: "A0+b0", Timestamp: 0},
		{Key: 0, Value: "B0+b0", Timestamp: 0},
		{Key: 1, Value: "A1+b1", Timestamp: 0},
		{Key: 1, Value: "B1+b1", Timestamp: 0},
		{Key: 2, Value: "B2+b2", Timestamp: 0},
		{Key: 3, Value: "B3+b3", Timestamp: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatal("should equal")
	}

	got = make([]commtypes.Message, 0)
	// push all four items to the primary stream; this should produce six items
	// w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3 }
	// w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
	// --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
	//     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
	for _, k := range expected_keys {
		ret := oneJoinTwo(ctx, commtypes.Message{Key: k, Value: fmt.Sprintf("C%d", k)})
		got = append(got, ret...)
	}
	expected_join = []commtypes.Message{
		{Key: 0, Value: "C0+a0", Timestamp: 0},
		{Key: 0, Value: "C0+b0", Timestamp: 0},
		{Key: 1, Value: "C1+a1", Timestamp: 0},
		{Key: 1, Value: "C1+b1", Timestamp: 0},
		{Key: 2, Value: "C2+b2", Timestamp: 0},
		{Key: 3, Value: "C3+b3", Timestamp: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}

	got = make([]commtypes.Message, 0)
	// push two items to the other stream; this should produce six items
	// w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
	// w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3 }
	// --> w1 = { 0:A0, 1:A1, 0:B0, 1:B1, 2:B2, 3:B3, 0:C0, 1:C1, 2:C2, 3:C3 }
	//     w2 = { 0:a0, 1:a1, 0:b0, 1:b1, 2:b2, 3:b3, 0:c0, 1:c1 }
	for i := 0; i < 2; i++ {
		ret := twoJoinOne(ctx, commtypes.Message{Key: expected_keys[i], Value: fmt.Sprintf("c%d", expected_keys[i])})
		got = append(got, ret...)
	}
	expected_join = []commtypes.Message{
		{Key: 0, Value: "A0+c0", Timestamp: 0},
		{Key: 0, Value: "B0+c0", Timestamp: 0},
		{Key: 0, Value: "C0+c0", Timestamp: 0},
		{Key: 1, Value: "A1+c1", Timestamp: 0},
		{Key: 1, Value: "B1+c1", Timestamp: 0},
		{Key: 1, Value: "C1+c1", Timestamp: 0},
	}
	if !reflect.DeepEqual(expected_join, got) {
		t.Fatalf("should equal. expected: %v, got: %v", expected_join, got)
	}
}
