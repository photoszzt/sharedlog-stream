//go:build !stats
// +build !stats

package producer_consumer

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
)

func assignInjTime(msg *commtypes.Message) {}
func extractProduceToConsumeTime(msgSeqs *commtypes.MsgAndSeqs, isInitialSrc bool, collector *stats.Int64Collector) error {
	return nil
}
