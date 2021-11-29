package benchutil

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream/processor"
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"sync"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
)

func GetShardedInputOutputStreams(ctx context.Context, env types.Environment, input *common.QueryInput) (*sharedlog_stream.ShardedSharedLogStream, *sharedlog_stream.ShardedSharedLogStream, error) {
	inputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.InputTopicName, uint8(input.NumInPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for input stream failed: %v", err)

	}
	outputStream, err := sharedlog_stream.NewShardedSharedLogStream(env, input.OutputTopicName, uint8(input.NumOutPartition))
	if err != nil {
		return nil, nil, fmt.Errorf("NewSharedlogStream for output stream failed: %v", err)
	}
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

type DumpOutputStreamConfig struct {
	MsgSerde      commtypes.MsgSerde
	KeySerde      commtypes.Serde
	ValSerde      commtypes.Serde
	OutputDir     string
	TopicName     string
	NumPartitions uint8
}

func DumpOutputStream(ctx context.Context, env types.Environment, args DumpOutputStreamConfig) error {
	log, err := sharedlog_stream.NewShardedSharedLogStream(env, args.TopicName, args.NumPartitions)
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
			fmt.Fprintf(os.Stderr, "before read next\n")
			_, rawMsgs, err := log.ReadNext(ctx, i)
			if errors.IsStreamEmptyError(err) {
				break
			}
			if err != nil {
				return err
			}
			for _, rawMsg := range rawMsgs {
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
	}
	return nil
}

func InvokeFunc(client *http.Client, response *common.FnOutput,
	wg *sync.WaitGroup, request interface{}, funcName string, gateway string,
) {
	defer wg.Done()
	url := utils.BuildFunctionUrl(gateway, funcName)
	if err := utils.JsonPostRequest(client, url, request, response); err != nil {
		log.Error().Msgf("%s request failed: %v", funcName, err)
	} else if !response.Success {
		log.Error().Msgf("%s request failed: %s", funcName, response.Message)
	}
}
