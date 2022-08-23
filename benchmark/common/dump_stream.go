package common

import (
	"context"
	"fmt"
	"os"
	"path"
	"sharedlog-stream/pkg/common_errors"
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/sharedlog_stream"

	"cs.utexas.edu/zjia/faas/types"
	"golang.org/x/xerrors"
)

type DumpOutputStreamConfig struct {
	MsgSerde      commtypes.MessageSerdeG[interface{}, interface{}]
	OutputDir     string
	TopicName     string
	SerdeFormat   commtypes.SerdeFormat
	NumPartitions uint8
}

func DumpOutputStream(ctx context.Context, env types.Environment, args DumpOutputStreamConfig) error {
	log, err := sharedlog_stream.NewShardedSharedLogStream(env, args.TopicName, args.NumPartitions, args.SerdeFormat)
	if err != nil {
		return err
	}
	/*
		epochMarkerSerde, err := commtypes.GetEpochMarkerSerdeG(args.SerdeFormat)
		if err != nil {
			return err
		}
	*/
	payloadArrSerde := sharedlog_stream.DEFAULT_PAYLOAD_ARR_SERDEG
	for i := uint8(0); i < args.NumPartitions; i++ {
		outFilePath := path.Join(args.OutputDir, fmt.Sprintf("%s-%d.txt", args.TopicName, i))
		outFile, err := os.Create(outFilePath)
		if err != nil {
			return err
		}
		for {
			// fmt.Fprintf(os.Stderr, "before read next\n")
			rawMsg, err := log.ReadNext(ctx, i)
			if err != nil {
				if xerrors.Is(err, common_errors.ErrStreamEmpty) {
					break
				}
				return err
			}
			if rawMsg.IsControl {
				/*
					epochMark, err := epochMarkerSerde.Decode(rawMsg.Payload)
					if err != nil {
						return err
					}
					outStr := fmt.Sprintf("%+v\n", epochMark)
					// fmt.Fprint(os.Stderr, outStr)
					writted, err := outFile.WriteString(outStr)
					if err != nil {
						return err
					}
					if writted != len(outStr) {
						panic("written is smaller than expected")
					}
				*/
			} else {
				msgAndSeq, err := commtypes.DecodeRawMsgG(rawMsg, args.MsgSerde, payloadArrSerde)
				if err != nil {
					return err
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
		if err := outFile.Close(); err != nil {
			print(err)
		}
	}
	return nil
}

func outputMsg(msg commtypes.Message, outFile *os.File) error {
	outStr := fmt.Sprintf("%v : %v, ts %d\n", msg.Key, msg.Value, msg.Timestamp)
	// fmt.Fprint(os.Stderr, outStr)
	writted, err := outFile.WriteString(outStr)
	if err != nil {
		return err
	}
	if writted != len(outStr) {
		panic("written is smaller than expected")
	}
	return nil
}
