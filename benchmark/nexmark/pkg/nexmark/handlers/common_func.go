package handlers

import (
	ntypes "sharedlog-stream/benchmark/nexmark/pkg/nexmark/types"

	"sharedlog-stream/pkg/stream/processor"
)

func only_bid(msg processor.Message) (bool, error) {
	event := msg.Value.(*ntypes.Event)
	return event.Etype == ntypes.BID, nil
}
