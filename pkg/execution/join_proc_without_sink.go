package execution

import (
	"context"
	"fmt"
	"os"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/errors"
	"sharedlog-stream/pkg/stream/processor/commtypes"

	"golang.org/x/xerrors"
)

func JoinProcSerialWithoutSink(
	ctx context.Context,
	procArgsTmp interface{},
) error {
	procArgs := procArgsTmp.(*JoinProcWithoutSinkArgs)
	gotMsgs, err := procArgs.src.Consume(ctx, procArgs.parNum)
	if err != nil {
		if xerrors.Is(err, errors.ErrStreamSourceTimeout) {
			debug.Fprintf(os.Stderr, "%s timeout, gen output\n", procArgs.src.TopicName())
			return err
		}
		return err
	}
	for _, msg := range gotMsgs.Msgs {
		if msg.MsgArr != nil {
			for _, subMsg := range msg.MsgArr {
				err = procMsg(ctx, subMsg, procArgs)
				if err != nil {
					return err
				}
			}
		} else {
			err = procMsg(ctx, msg.Msg, procArgs)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func procMsg(ctx context.Context, msg commtypes.Message, procArgs *JoinProcWithoutSinkArgs) error {
	st := msg.Value.(commtypes.StreamTimeExtractor)
	ts, err := st.ExtractStreamTime()
	if err != nil {
		return fmt.Errorf("fail to extract timestamp: %v", err)
	}
	msg.Timestamp = ts
	_, err = procArgs.runner(ctx, msg)
	if err != nil {
		return err
	}
	return nil
}
