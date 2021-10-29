package processor

import "sharedlog-stream/pkg/stream/processor/commtypes"

type Sink interface {
	Sink(msg commtypes.Message, parNum uint8) error
}
