package execution

import (
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/proc_interface"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type CommonJoinProcArgs struct {
	arg1     *JoinProcArgs
	arg2     *JoinProcArgs
	outChan1 <-chan *common.FnOutput
	outChan2 <-chan *common.FnOutput
	proc_interface.BaseExecutionContext
}

func NewCommonJoinProcArgs(
	arg1 *JoinProcArgs,
	arg2 *JoinProcArgs,
	outChan1 <-chan *common.FnOutput,
	outChan2 <-chan *common.FnOutput,
	ectx proc_interface.BaseExecutionContext,
) *CommonJoinProcArgs {
	return &CommonJoinProcArgs{
		arg1:                 arg1,
		arg2:                 arg2,
		outChan1:             outChan1,
		outChan2:             outChan2,
		BaseExecutionContext: ectx,
	}
}

func (c *CommonJoinProcArgs) SetRecordFinishFunc(recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
	c.BaseProcArgs.SetRecordFinishFunc(recordFinishFunc)
	c.arg1.SetRecordFinishFunc(recordFinishFunc)
	c.arg2.SetRecordFinishFunc(recordFinishFunc)
}

func (c *CommonJoinProcArgs) SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) {
	c.BaseProcArgs.SetTrackParFunc(trackParFunc)
	c.arg1.SetTrackParFunc(trackParFunc)
	c.arg2.SetTrackParFunc(trackParFunc)
}

func HandleJoinErrReturn(argsTmp interface{}) *common.FnOutput {
	args := argsTmp.(*CommonJoinProcArgs)
	var out1 *common.FnOutput
	var out2 *common.FnOutput
	select {
	case out2 = <-args.outChan2:
		debug.Fprintf(os.Stderr, "Got out2 out: %v, per out channel len: %d\n", out2, len(args.outChan2))
	case out1 = <-args.outChan1:
		debug.Fprintf(os.Stderr, "Got out1 out: %v, auc out channel len: %d\n", out1, len(args.outChan1))
	default:
	}
	if out1 != nil || out2 != nil {
		debug.Fprintf(os.Stderr, "out1: %v\n", out1)
		debug.Fprintf(os.Stderr, "out2: %v\n", out2)
	}
	if out2 != nil && !out2.Success {
		return out2
	}
	if out1 != nil && !out1.Success {
		return out1
	}
	return nil
}
