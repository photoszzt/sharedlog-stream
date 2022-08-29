//go:build !stats
// +build !stats

package producer_consumer

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
)

func assignInjTime(msg *commtypes.MessageSerialized) error {
	return nil
}
func extractProduceToConsumeTime(msgSeqs *commtypes.Message, isInitialSrc bool, collector *stats.StatsCollector[int64]) error {
	return nil
}

func extractProduceToConsumeTimeMsgG[K, V any](msg *commtypes.MessageG[K, V], isInitialSrc bool, collector *stats.StatsCollector[int64]) {
}
