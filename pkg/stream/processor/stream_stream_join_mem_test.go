package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"testing"
	"time"
)

func getStreamJoinMem(joinWindows *JoinWindows, t *testing.T) (
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
) {
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
	toWinTab1, winTab1, err := ToInMemWindowTable("tab1", joinWindows, compare, time.Duration(0))
	if err != nil {
		t.Fatal(err.Error())
	}
	toWinTab2, winTab2, err := ToInMemWindowTable("tab2", joinWindows, compare, time.Duration(0))
	if err != nil {
		t.Fatal(err.Error())
	}
	joiner := ValueJoinerWithKeyTsFunc(
		func(readOnlyKey interface{},
			leftValue interface{}, rightValue interface{}, leftTs int64, rightTs int64,
		) interface{} {
			debug.Fprintf(os.Stderr, "left val: %v, ts: %d, right val: %v, ts: %d\n", leftValue, leftTs, rightValue, rightTs)
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

func TestStreamStreamJoinMem(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	oneJoinTwo, twoJoinOne := getStreamJoinMem(joinWindows, t)
	StreamStreamJoin(ctx, oneJoinTwo, twoJoinOne, t)
}

func TestWindowingMem(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(100)*time.Millisecond, time.Duration(100)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	oneJoinTwo, twoJoinOne := getStreamJoinMem(joinWindows, t)
	Windowing(ctx, oneJoinTwo, twoJoinOne, t)
}

func TestAsymmetricWindowingAfterMem(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsNoGrace(time.Duration(0) * time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	joinWindows, err = joinWindows.After(time.Duration(100) * time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	debug.Fprintf(os.Stderr, "join windows: before %d, after %d, grace %d\n", joinWindows.beforeMs,
		joinWindows.afterMs, joinWindows.graceMs)
	oneJoinTwo, twoJoinOne := getStreamJoinMem(joinWindows, t)
	AsymmetricWindowingAfter(ctx, oneJoinTwo, twoJoinOne, t)
}

func TestAsymmetricWindowingBeforeMem(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(0)*time.Millisecond,
		time.Duration(86400000)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	joinWindows, err = joinWindows.Before(time.Duration(100) * time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	debug.Fprintf(os.Stderr, "join windows: before %d, after %d, grace %d\n", joinWindows.beforeMs,
		joinWindows.afterMs, joinWindows.graceMs)
	oneJoinTwo, twoJoinOne := getStreamJoinMem(joinWindows, t)
	AsymmetricWindowingBefore(ctx, oneJoinTwo, twoJoinOne, t)
}
