//go:build !stats
// +build !stats

package source_sink

import (
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/stream/processor/commtypes"
)

func assignInjTime(msg *commtypes.Message) {}
func extractProduceToConsumeTime(msgSeqs *commtypes.MsgAndSeqs, isInitialSrc bool, collector *stats.Int64Collector) error {
	return nil
}
