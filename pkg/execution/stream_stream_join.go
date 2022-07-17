package execution

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/concurrent_skiplist"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_restore"
	"sharedlog-stream/pkg/store_with_changelog"
)

func SetupStreamStreamJoin(
	mpLeft *store_with_changelog.MaterializeParam,
	mpRight *store_with_changelog.MaterializeParam,
	compare concurrent_skiplist.CompareFunc,
	joiner processor.ValueJoinerWithKeyTsFunc,
	jw *processor.JoinWindows,
) (proc_interface.ProcessAndReturnFunc,
	proc_interface.ProcessAndReturnFunc,
	[]*store_restore.WindowStoreChangelog,
	error,
) {
	toLeftTab, leftTab, err := store_with_changelog.ToInMemWindowTableWithChangelog(
		mpLeft, jw, compare)
	if err != nil {
		return nil, nil, nil, err
	}
	toRightTab, rightTab, err := store_with_changelog.ToInMemWindowTableWithChangelog(
		mpRight, jw, compare)
	if err != nil {
		return nil, nil, nil, err
	}
	leftJoinRight, rightJoinLeft := configureStreamStreamJoinProcessor(leftTab, rightTab, joiner, jw)
	leftJoinRightFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		// debug.Fprintf(os.Stderr, "before toLeft\n")
		rets, err := toLeftTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		// debug.Fprintf(os.Stderr, "after toLeft\n")
		msgs, err := leftJoinRight.ProcessAndReturn(ctx, rets[0])
		// debug.Fprintf(os.Stderr, "after leftJoinRight\n")
		return msgs, err
	}
	rightJoinLeftFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		// debug.Fprintf(os.Stderr, "before toRight\n")
		rets, err := toRightTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		// debug.Fprintf(os.Stderr, "after toRight\n")
		msgs, err := rightJoinLeft.ProcessAndReturn(ctx, rets[0])
		// debug.Fprintf(os.Stderr, "after rightJoinLeft\n")
		return msgs, err
	}
	wsc := []*store_restore.WindowStoreChangelog{
		store_restore.NewWindowStoreChangelog(leftTab, leftTab.ChangelogManager()),
		store_restore.NewWindowStoreChangelog(rightTab, rightTab.ChangelogManager()),
	}
	return leftJoinRightFunc, rightJoinLeftFunc, wsc, nil
}

func configureStreamStreamJoinProcessor(leftTab store.WindowStore,
	rightTab store.WindowStore,
	joiner processor.ValueJoinerWithKeyTsFunc,
	jw *processor.JoinWindows,
) (*processor.MeteredProcessor, *processor.MeteredProcessor) {
	sharedTimeTracker := processor.NewTimeTracker()
	leftJoinRight := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor("leftJoinRight", rightTab, jw, joiner, false, true, sharedTimeTracker),
	)
	rightJoinLeft := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor("rightJoinLeft", leftTab, jw,
			processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker),
	)
	return leftJoinRight, rightJoinLeft
}
