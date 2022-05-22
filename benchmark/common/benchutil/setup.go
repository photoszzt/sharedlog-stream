package benchutil

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sharedlog-stream/pkg/transaction"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
)

type DumpOutputStreamConfig struct {
	MsgSerde      commtypes.MsgSerde
	KeySerde      commtypes.Serde
	ValSerde      commtypes.Serde
	OutputDir     string
	TopicName     string
	SerdeFormat   commtypes.SerdeFormat
	NumPartitions uint8
}

func GetShardedInputOutputStreams(ctx context.Context,
	env types.Environment,
	input *common.QueryInput,
) (*sharedlog_stream.ShardedSharedLogStream, []*sharedlog_stream.ShardedSharedLogStream, error) {
	inputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicNames[0], input.NumInPartition,
		commtypes.SerdeFormat(input.SerdeFormat))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}
	var output_streams []*sharedlog_stream.ShardedSharedLogStream
	for idx, name := range input.OutputTopicNames {
		outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, name, input.NumOutPartitions[idx],
			commtypes.SerdeFormat(input.SerdeFormat))
		if err != nil {
			return nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
		}
		output_streams = append(output_streams, outputStream)
	}
	return inputStream, output_streams, nil
}

func DumpOutputStream(ctx context.Context, env types.Environment, args DumpOutputStreamConfig) error {
	log, err := sharedlog_stream.NewShardedSharedLogStream(env, args.TopicName, args.NumPartitions, args.SerdeFormat)
	if err != nil {
		return err
	}
	for i := uint8(0); i < args.NumPartitions; i++ {
		outFilePath := path.Join(args.OutputDir, fmt.Sprintf("%s-%d.txt", args.TopicName, i))
		outFile, err := os.Create(outFilePath)
		defer func() {
			if err := outFile.Close(); err != nil {
				panic(err)
			}
		}()
		if err != nil {
			return err
		}
		for {
			// fmt.Fprintf(os.Stderr, "before read next\n")
			rawMsg, err := log.ReadNext(ctx, i)
			if errors.IsStreamEmptyError(err) {
				break
			}
			if err != nil {
				return err
			}
			keyBytes, valBytes, err := args.MsgSerde.Decode(rawMsg.Payload)
			if err != nil {
				return err
			}
			key, err := args.KeySerde.Decode(keyBytes)
			if err != nil {
				return err
			}
			val, err := args.ValSerde.Decode(valBytes)
			if err != nil {
				return err
			}
			outStr := fmt.Sprintf("%v, %v\n", key, val)
			fmt.Fprint(os.Stderr, outStr)
			writted, err := outFile.WriteString(outStr)
			if err != nil {
				return err
			}
			if writted != len(outStr) {
				panic("written is smaller than expected")
			}
		}
	}
	return nil
}

func InvokeFunc(client *http.Client, response *common.FnOutput,
	wg *sync.WaitGroup, request interface{}, funcName string, gateway string,
) {
	defer wg.Done()
	url := utils.BuildFunctionUrl(gateway, funcName)
	if err := utils.JsonPostRequest(client, url, "", request, response); err != nil {
		log.Error().Msgf("%s request failed: %v", funcName, err)
	} else if !response.Success {
		log.Error().Msgf("%s request failed: %s", funcName, response.Message)
	}
}

func UpdateStreamTaskArgsTransaction(sp *common.QueryInput, args *transaction.StreamTaskArgsTransaction) {
	debug.Assert(sp.AppId != "", "app id should not be empty")
	args.WithAppID(sp.AppId).
		WithWarmup(time.Duration(sp.WarmupS) * time.Second).
		WithScaleEpoch(sp.ScaleEpoch).
		WithCommitEveryMs(sp.CommitEveryMs).
		WithCommitEveryNIter(sp.CommitEveryNIter).
		WithExitAfterNCommit(sp.ExitAfterNCommit).
		WithDuration(sp.Duration).
		WithSerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		WithInParNum(sp.ParNum)
}

func UpdateStreamTaskArgs(sp *common.QueryInput, args *transaction.StreamTaskArgs) {
	args.WithDuration(time.Duration(sp.Duration) * time.Second).
		WithNumInPartition(sp.NumInPartition).
		WithParNum(sp.ParNum).
		WithSerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).
		WithWarmup(time.Duration(sp.WarmupS) * time.Second).
		WithFlushEvery(time.Duration(sp.FlushMs) * time.Millisecond)
}
