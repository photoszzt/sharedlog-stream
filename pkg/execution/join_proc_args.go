package execution

import (
	"sharedlog-stream/pkg/hash"
	"sharedlog-stream/pkg/source_sink"
	"sharedlog-stream/pkg/transaction/tran_interface"
	"sync"
)

type JoinProcArgs struct {
	src          source_sink.Source
	sink         source_sink.Sink
	trackParFunc tran_interface.TrackKeySubStreamFunc

	runner  JoinWorkerFunc
	cHashMu *sync.RWMutex
	cHash   *hash.ConsistentHash
	parNum  uint8
}

func NewJoinProcArgs(src source_sink.Source,
	sink source_sink.Sink,
	runner JoinWorkerFunc, cHashMu *sync.RWMutex,
	cHash *hash.ConsistentHash,
	parNum uint8,
) *JoinProcArgs {
	return &JoinProcArgs{
		src:          src,
		sink:         sink,
		trackParFunc: tran_interface.DefaultTrackSubstreamFunc,
		runner:       runner,
		cHashMu:      cHashMu,
		cHash:        cHash,
		parNum:       parNum,
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
