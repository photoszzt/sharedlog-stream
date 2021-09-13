package state

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/processor"

type InMemoryWindowStore struct {
	Name       string
	WindowSize uint64
	pctx       processor.ProcessorContext
}
