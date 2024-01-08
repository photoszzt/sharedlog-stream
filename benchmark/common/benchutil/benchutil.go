package benchutil

import (
	"context"
	"fmt"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream_task"
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

func UpdateStreamTaskArgs(sp *common.QueryInput, argsBuilder stream_task.SetGuarantee) stream_task.BuildStreamTaskArgs {
	debug.Assert(sp.AppId != "", "app id should not be empty")
	ret := argsBuilder.Guarantee(exactly_once_intr.GuaranteeMth(sp.GuaranteeMth)).
		AppID(sp.AppId).
		Warmup(time.Duration(sp.WarmupS) * time.Second).
		CommitEveryMs(sp.CommitEveryMs).
		FlushEveryMs(sp.FlushMs).
		Duration(sp.Duration).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		BufMaxSize(sp.BufMaxSize).
		WaitEndMark(sp.WaitForEndMark)
	if sp.TestParams != nil {
		ret.TestParams(sp.TestParams)
	}
	if sp.SnapEveryS != 0 {
		ret.SnapshotEveryS(sp.SnapEveryS)
	}
	return ret
}

func GetShardedInputOutputStreams(ctx context.Context,
	env types.Environment,
	input *common.QueryInput,
) (*sharedlog_stream.ShardedSharedLogStream, []*sharedlog_stream.ShardedSharedLogStream, error) {
	serdeFormat := commtypes.SerdeFormat(input.SerdeFormat)
	inputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[0], input.NumInPartition,
		serdeFormat, input.BufMaxSize)
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)
	}
	var output_streams []*sharedlog_stream.ShardedSharedLogStream
	for idx, name := range input.OutputTopicNames {
		outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, name, input.NumOutPartitions[idx],
			serdeFormat, input.BufMaxSize)
		if err != nil {
			return nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
		}
		output_streams = append(output_streams, outputStream)
	}
	return inputStream, output_streams, nil
}

func UseCache(useCache bool, gua uint8) bool {
	c := useCache
	g := exactly_once_intr.GuaranteeMth(gua)
	if g == exactly_once_intr.ALIGN_CHKPT {
		c = false
	}
	return c
}
