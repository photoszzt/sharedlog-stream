//go:build !stats
// +build !stats

package producer_consumer

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
)

func assignInjTime(msg *commtypes.Message) error {
	return nil
}
func extractProduceToConsumeTime(msgSeqs *commtypes.Message, isInitialSrc bool, collector *stats.StatsCollector[int64]) error {
	return nil
}
