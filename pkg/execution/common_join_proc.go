package execution

import (
	"os"
	"sharedlog-stream/benchmark/common"
	"sharedlog-stream/pkg/debug"
	"sharedlog-stream/pkg/transaction"
	"sharedlog-stream/pkg/transaction/tran_interface"
)

type CommonJoinProcArgs struct {
	outChan2         <-chan *common.FnOutput
	outChan1         <-chan *common.FnOutput
	recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc
	funcName         string
	curEpoch         uint64
	parNum           uint8
}

func NewCommonJoinProcArgs(outChan1 <-chan *common.FnOutput,
	outChan2 <-chan *common.FnOutput, funcName string, curEpoch uint64, parNum uint8,
) *CommonJoinProcArgs {
	return &CommonJoinProcArgs{
		outChan2:         outChan2,
		outChan1:         outChan1,
		recordFinishFunc: transaction.DefaultRecordPrevInstanceFinishFunc,
		funcName:         funcName,
		curEpoch:         curEpoch,
		parNum:           parNum,
	}
}

func (a *CommonJoinProcArgs) ParNum() uint8    { return a.parNum }
func (a *CommonJoinProcArgs) CurEpoch() uint64 { return a.curEpoch }
func (a *CommonJoinProcArgs) FuncName() string { return a.funcName }
func (a *CommonJoinProcArgs) RecordFinishFunc() tran_interface.RecordPrevInstanceFinishFunc {
	return a.recordFinishFunc
}

func (a *CommonJoinProcArgs) SetRecordFinishFunc(recordFinishFunc tran_interface.RecordPrevInstanceFinishFunc) {
	a.recordFinishFunc = recordFinishFunc
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
