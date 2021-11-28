package benchutil

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"cs.utexas.edu/zjia/faas/types"
)

func GetShardedInputOutputStreams(ctx context.Context, env types.Environment, input *common.QueryInput) (*sharedlog_stream.ShardedSharedLogStream, *sharedlog_stream.ShardedSharedLogStream, error) {
	inputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicName, uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}
	/*
		err = inputStream.InitStream(ctx, true)
		if err != nil {
			return nil, nil, fmt.Errorf("InitStream failed: %v", err)
		}
	*/
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
	/*
		err = outputStream.InitStream(ctx, false)
		if err != nil {
			return nil, nil, fmt.Errorf("InitStream failed: %v", err)
		}
	*/
	return inputStream, outputStream, nil
}

func SetupTransactionManager(ctx context.Context, env types.Environment,
	transactionalId string, sp *common.QueryInput, src processor.Source,
) (*sharedlog_stream.TransactionManager, uint64, uint16, error) {
	tm, err := sharedlog_stream.NewTransactionManager(ctx, env, transactionalId, commtypes.SerdeFormat(sp.SerdeFormat))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("NewTransactionManager failed: %v", err)
	}
	appId, appEpoch, err := tm.InitTransaction(ctx)
	if err != nil {
		return nil, 0, 0, fmt.Errorf("InitTransaction failed: %v", err)
	}

	err = tm.CreateOffsetTopic(sp.InputTopicName, uint8(sp.NumInPartition))
	if err != nil {
		return nil, 0, 0, fmt.Errorf("create offset topic failed: %v", err)
	}

	offset, err := tm.FindLastOffset(ctx, sp.InputTopicName, sp.ParNum)
	if err != nil {
		if !errors.IsStreamEmptyError(err) {
			return nil, 0, 0, err
		}
	}
	if offset != 0 {
		src.SetCursor(offset+1, sp.ParNum)
	}
	return tm, appId, appEpoch, nil
}

func TrackOffsetAndCommit(ctx context.Context,
	offsetConfig sharedlog_stream.OffsetConfig,
	tm *sharedlog_stream.TransactionManager, hasLiveTransaction *bool, trackConsumePar *bool,
	retc chan *common.FnOutput,
) {
	err := tm.AppendOffset(ctx, offsetConfig)
	if err != nil {
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("append offset failed: %v\n", err),
		}
	}
	err = tm.CommitTransaction(ctx)
	if err != nil {
		retc <- &common.FnOutput{
			Success: false,
			Message: fmt.Sprintf("commit failed: %v\n", err),
		}
	}
	*hasLiveTransaction = false
	*trackConsumePar = false
}
