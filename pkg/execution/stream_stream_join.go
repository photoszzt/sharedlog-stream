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
		_, err := toLeftTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		return leftJoinRight.ProcessAndReturn(ctx, msg)
	}
	rightJoinLeftFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		_, err := toRightTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		return rightJoinLeft.ProcessAndReturn(ctx, msg)
	}
	wsc := []*store_restore.WindowStoreChangelog{
		store_restore.NewWindowStoreChangelog(leftTab, mpLeft.ChangelogManager(), mpLeft.ParNum()),
		store_restore.NewWindowStoreChangelog(rightTab, mpRight.ChangelogManager(), mpRight.ParNum()),
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
