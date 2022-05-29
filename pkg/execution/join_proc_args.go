package execution

import (
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/stream/processor/proc_interface"
	"sync"
)

type JoinProcArgs struct {
	runner  JoinWorkerFunc
	cHashMu *sync.RWMutex
	cHash   *hash.ConsistentHash
	proc_interface.BaseProcArgsWithSrcSink
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
		runner:  runner,
		cHashMu: cHashMu,
		cHash:   cHash,
		BaseProcArgsWithSrcSink: proc_interface.NewBaseProcArgsWithSrcSink(
			src,
			[]source_sink.Sink{sink}, funcName, curEpoch, parNum),
	}
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
