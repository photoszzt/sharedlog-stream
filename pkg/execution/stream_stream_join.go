package execution

import (
	"context"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/processor"
	"sharedlog-stream/pkg/store"
	"sharedlog-stream/pkg/store_with_changelog"
	"sharedlog-stream/pkg/utils"
)

func SetupStreamStreamJoin[K, VLeft, VRight any](
	mpLeft *store_with_changelog.MaterializeParam[K, VLeft],
	mpRight *store_with_changelog.MaterializeParam[K, VRight],
	compare store.CompareFunc,
	joiner processor.ValueJoinerWithKeyTsFunc,
	jw *processor.JoinWindows,
) (proc_interface.ProcessAndReturnFunc,
	proc_interface.ProcessAndReturnFunc,
	map[string]store.WindowStoreOpWithChangelog,
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
	sharedTimeTracker := processor.NewTimeTracker()
	leftJoinRight := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor("leftJoinRight", rightTab, jw, joiner, false, true, sharedTimeTracker),
	)
	rightJoinLeft := processor.NewMeteredProcessor(
		processor.NewStreamStreamJoinProcessor("rightJoinLeft", leftTab, jw,
			processor.ReverseValueJoinerWithKeyTs(joiner), false, false, sharedTimeTracker),
	)
	nullKeyFilter := processor.NewMeteredProcessor(processor.NewStreamFilterProcessor("filterNullKey",
		processor.PredicateFunc(func(key interface{}, value interface{}) (bool, error) {
			return !utils.IsNil(key), nil
		})))
	leftJoinRightFunc := func(ctx context.Context, msg commtypes.Message) ([]commtypes.Message, error) {
		// debug.Fprintf(os.Stderr, "before toLeft\n")
		rets, err := toLeftTab.ProcessAndReturn(ctx, msg)
		if err != nil {
			return nil, err
		}
		// debug.Fprintf(os.Stderr, "after toLeft\n")
		msgs, err := leftJoinRight.ProcessAndReturn(ctx, rets[0])
		// debug.Fprintf(os.Stderr, "after leftJoinRight\n")
		out := make([]commtypes.Message, 0, len(msgs))
		for _, msg := range msgs {
			ret, err := nullKeyFilter.ProcessAndReturn(ctx, msg)
			if err != nil {
				return nil, err
			}
			out = append(out, ret...)
		}
		return out, err
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
		out := make([]commtypes.Message, 0, len(msgs))
		for _, msg := range msgs {
			ret, err := nullKeyFilter.ProcessAndReturn(ctx, msg)
			if err != nil {
				return nil, err
			}
			out = append(out, ret...)
		}
		return out, err
	}
	wsc := map[string]store.WindowStoreOpWithChangelog{
		leftTab.ChangelogTopicName():  leftTab,
		rightTab.ChangelogTopicName(): rightTab}
	return leftJoinRightFunc, rightJoinLeftFunc, wsc, nil
}
