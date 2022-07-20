//go:build !stats
// +build !stats

package producer_consumer

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
)

func assignInjTime(msg *commtypes.Message[any, any]) error {
	return nil
}
func extractProduceToConsumeTime(msgSeqs *commtypes.Message[any, any], isInitialSrc bool, collector *stats.Int64Collector) error {
	return nil
}
