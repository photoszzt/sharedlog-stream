package benchutil

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/benchmark/nexmark/pkg/nexmark/utils"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/exactly_once_intr"
	"sharedlog-stream/pkg/producer_consumer"
	"sharedlog-stream/pkg/sharedlog_stream"
	"sharedlog-stream/pkg/stream_task"
	"sync"
	"time"

	"cs.utexas.edu/zjia/faas/types"
	"github.com/rs/zerolog/log"
	"golang.org/x/xerrors"
)

type DumpOutputStreamConfig struct {
	MsgSerde      commtypes.MessageSerdeG[interface{}, interface{}]
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
	src, err := producer_consumer.NewShardedSharedLogStreamConsumerG(log, &producer_consumer.StreamConsumerConfigG[interface{}, interface{}]{
		MsgSerde:    args.MsgSerde,
		Timeout:     common.SrcConsumeTimeout,
		SerdeFormat: args.SerdeFormat,
	})
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
			msgAndSeqs, err := src.Consume(ctx, i)
			if xerrors.Is(err, common_errors.ErrStreamSourceTimeout) {
				break
			}
			if err != nil {
				return err
			}
			msgAndSeq := msgAndSeqs.Msgs
			if msgAndSeq.IsControl {
				continue
			}
			if msgAndSeq.MsgArr != nil {
				for _, msg := range msgAndSeq.MsgArr {
					err = outputMsg(msg, outFile)
					if err != nil {
						return err
					}
				}
			} else {
				err = outputMsg(msgAndSeq.Msg, outFile)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func outputMsg(msg commtypes.Message, outFile *os.File) error {
	outStr := fmt.Sprintf("%v : %v, ts %d\n", msg.Key, msg.Value, msg.Timestamp)
	fmt.Fprint(os.Stderr, outStr)
	writted, err := outFile.WriteString(outStr)
	if err != nil {
		return err
	}
	if writted != len(outStr) {
		panic("written is smaller than expected")
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

func UpdateStreamTaskArgs(sp *common.QueryInput, argsBuilder stream_task.SetGuarantee) stream_task.BuildStreamTaskArgs {
	debug.Assert(sp.AppId != "", "app id should not be empty")
	return argsBuilder.Guarantee(exactly_once_intr.GuaranteeMth(sp.GuaranteeMth)).
		AppID(sp.AppId).
		Warmup(time.Duration(sp.WarmupS) * time.Second).
		CommitEveryMs(sp.CommitEveryMs).
		FlushEveryMs(sp.FlushMs).
		Duration(sp.Duration).
		SerdeFormat(commtypes.SerdeFormat(sp.SerdeFormat)).WaitEndMark(sp.WaitForEndMark)
}
