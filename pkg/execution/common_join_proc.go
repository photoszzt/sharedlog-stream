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
	proc_interface.BaseSrcsSinks
}

func NewCommonJoinProcArgs(
	arg1 *JoinProcArgs,
	arg2 *JoinProcArgs,
	outChan1 <-chan *common.FnOutput,
	outChan2 <-chan *common.FnOutput,
	ss proc_interface.BaseSrcsSinks,
) *CommonJoinProcArgs {
	return &CommonJoinProcArgs{
		arg1:          arg1,
		arg2:          arg2,
		outChan1:      outChan1,
		outChan2:      outChan2,
		BaseSrcsSinks: ss,
	}
}

func (c *CommonJoinProcArgs) SetRecordFinishFunc(recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
	c.arg1.SetRecordFinishFunc(recordFinishFunc)
	c.arg2.SetRecordFinishFunc(recordFinishFunc)
}

func (c *CommonJoinProcArgs) SetTrackParFunc(trackParFunc tran_interface.TrackProdSubStreamFunc) {
	c.arg1.SetTrackParFunc(trackParFunc)
	c.arg2.SetTrackParFunc(trackParFunc)
}

// arg1 and arg2 shared the same param for the following functions
func (c *CommonJoinProcArgs) RecordFinishFunc() tran_interface.RecordPrevInstanceFinishFunc {
	return c.arg1.RecordFinishFunc()
}

func (c *CommonJoinProcArgs) TrackParFunc() tran_interface.TrackProdSubStreamFunc {
	return c.arg1.TrackParFunc()
}

func (c *CommonJoinProcArgs) FuncName() string {
	return c.arg1.FuncName()
}

func (c *CommonJoinProcArgs) CurEpoch() uint64 {
	return c.arg1.CurEpoch()
}

func (c *CommonJoinProcArgs) SubstreamNum() uint8 {
	return c.arg1.SubstreamNum()
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
