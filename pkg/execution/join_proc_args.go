package execution

import (
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sync"
)

type JoinProcArgs struct {
	src          source_sink.Source
	trackParFunc tran_interface.TrackKeySubStreamFunc

	runner  JoinWorkerFunc
	cHashMu *sync.RWMutex
	cHash   *hash.ConsistentHash
	proc_interface.BaseProcArgsWithSink
}

// var _ = proc_interface.ProcArgsWithSink(&JoinProcArgs{})

func NewJoinProcArgs(
	src source_sink.Source,
	sink source_sink.Sink,
	runner JoinWorkerFunc,
	cHashMu *sync.RWMutex,
	cHash *hash.ConsistentHash,
	funcName string,
	curEpoch uint64,
	parNum uint8,
) *JoinProcArgs {
	return &JoinProcArgs{
		src:                  src,
		trackParFunc:         tran_interface.DefaultTrackSubstreamFunc,
		runner:               runner,
		cHashMu:              cHashMu,
		cHash:                cHash,
		BaseProcArgsWithSink: proc_interface.NewBaseProcArgsWithSink([]source_sink.Sink{sink}, funcName, curEpoch, parNum),
	}
}

func (args *JoinProcArgs) SetTrackParFunc(trackParFunc tran_interface.TrackKeySubStreamFunc) {
	args.trackParFunc = trackParFunc
}

type JoinProcWithoutSinkArgs struct {
	src    source_sink.Source
	runner JoinWorkerFunc
	parNum uint8
}

func NewJoinProcWithoutSinkArgs(src source_sink.Source, runner JoinWorkerFunc, parNum uint8) *JoinProcWithoutSinkArgs {
	return &JoinProcWithoutSinkArgs{
		src:    src,
		runner: runner,
		parNum: parNum,
	}
}
