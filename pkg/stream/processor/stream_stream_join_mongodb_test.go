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

func getStreamJoinMongoDB(ctx context.Context, joinWindows *JoinWindows, dbName string, t *testing.T) (
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
	func(ctx context.Context, m commtypes.Message) []commtypes.Message,
	*store.MongoDBKeyValueStore,
) {
	mkvs, err := store.NewMongoDBKeyValueStore(ctx, &store.MongoDBConfig{
		Addr:           "mongodb://localhost:27017",
		CollectionName: "a",
		KeySerde:       commtypes.IntSerde{},
		ValueSerde:     commtypes.StringSerde{},
		DBName:         dbName,
		StoreName:      "test1",
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	toWinTab1, winTab1, err := ToMongoDBWindowTable(ctx, "tab1", mkvs, joinWindows, commtypes.IntSerde{}, commtypes.StringSerde{})
	if err != nil {
		t.Fatal(err.Error())
	}
	toWinTab2, winTab2, err := ToMongoDBWindowTable(ctx, "tab2", mkvs, joinWindows, commtypes.IntSerde{}, commtypes.StringSerde{})
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
	err = mkvs.DropDatabase(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}
	return oneJoinTwo, twoJoinOne, mkvs
}

func TestStreamStreamJoinMongoDB(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(50)*time.Millisecond,
		time.Duration(50)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	oneJoinTwo, twoJoinOne, mkvs := getStreamJoinMongoDB(ctx, joinWindows, "db1", t)
	StreamStreamJoin(ctx, oneJoinTwo, twoJoinOne, t)
	err = mkvs.DropDatabase(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}
}

func TestWindowingMongoDB(t *testing.T) {
	ctx := context.Background()
	joinWindows, err := NewJoinWindowsWithGrace(time.Duration(100)*time.Millisecond, time.Duration(100)*time.Millisecond)
	if err != nil {
		t.Fatal(err.Error())
	}
	oneJoinTwo, twoJoinOne, mkvs := getStreamJoinMongoDB(ctx, joinWindows, "db2", t)
	Windowing(ctx, oneJoinTwo, twoJoinOne, t)
	err = mkvs.DropDatabase(ctx)
	if err != nil {
		t.Fatal(err.Error())
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
	oneJoinTwo, twoJoinOne, mkvs := getStreamJoinMongoDB(ctx, joinWindows, "db3", t)
	AsymmetricWindowingAfter(ctx, oneJoinTwo, twoJoinOne, t)
	err = mkvs.DropDatabase(ctx)
	if err != nil {
		t.Fatal(err.Error())
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
	oneJoinTwo, twoJoinOne, mkvs := getStreamJoinMongoDB(ctx, joinWindows, "db4", t)
	AsymmetricWindowingBefore(ctx, oneJoinTwo, twoJoinOne, t)
	err = mkvs.DropDatabase(ctx)
	if err != nil {
		t.Fatal(err.Error())
	}
}
