package processor

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/stream/processor/store"
	"testing"
	"time"
)

func getStreamJoinMongoDB(ctx context.Context, joinWindows *JoinWindows, dbName1 string, dbName2 string, t *testing.T) (
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
	store.WindowStore,
	store.WindowStore,
) {
	mongoAddr := "mongodb://localhost:27017"
	toWinTab1, winTab1, err := ToMongoDBWindowTable(ctx, dbName1, mongoAddr, joinWindows, commtypes.IntSerde{}, commtypes.StringSerde{})
	if err != nil {
		t.Fatal(err.Error())
	}
	toWinTab2, winTab2, err := ToMongoDBWindowTable(ctx, dbName2, mongoAddr, joinWindows, commtypes.IntSerde{}, commtypes.StringSerde{})
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
	if err = winTab1.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
	if err = winTab2.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
	return oneJoinTwo, twoJoinOne, winTab1, winTab2
}

func TestStreamStreamJoinMongoDB(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	oneJoinTwo, twoJoinOne, winTab1, winTab2 := getStreamJoinMongoDB(ctx, joinWindows, "db11", "db12", t)
	StreamStreamJoin(ctx, oneJoinTwo, twoJoinOne, t)
	if err = winTab1.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
	if err = winTab2.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestWindowingMongoDB(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(100)*time.Millisecond, time.Duration(100)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	oneJoinTwo, twoJoinOne, winTab1, winTab2 := getStreamJoinMongoDB(ctx, joinWindows, "db21", "db22", t)
	Windowing(ctx, oneJoinTwo, twoJoinOne, t)
	if err = winTab1.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
	if err = winTab2.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAsymmetricWindowingAfterMongoDB(t *testing.T) {
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
	oneJoinTwo, twoJoinOne, winTab1, winTab2 := getStreamJoinMongoDB(ctx, joinWindows, "db31", "dn32", t)
	AsymmetricWindowingAfter(ctx, oneJoinTwo, twoJoinOne, t)
	if err = winTab1.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
	if err = winTab2.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestAsymmetricWindowingBeforeMongoDB(t *testing.T) {
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
	oneJoinTwo, twoJoinOne, winTab1, winTab2 := getStreamJoinMongoDB(ctx, joinWindows, "db41", "db42", t)
	AsymmetricWindowingBefore(ctx, oneJoinTwo, twoJoinOne, t)
	if err = winTab1.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
	if err = winTab2.DropDatabase(ctx); err != nil {
		t.Fatal(err)
	}
}
