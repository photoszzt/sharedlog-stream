package processor

import "cs.utexas.edu/zhitingz/sharedlog-stream/pkg/stream/state"

type ProcessorContext interface {
	RegisterStateStore(store state.StateStore)
}
