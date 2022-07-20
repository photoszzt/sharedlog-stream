//go:build stats
// +build stats

package producer_consumer

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
	"time"
)

func assignInjTime[KeyT, ValT any](msg *commtypes.Message) {
	nowMs := time.Now().UnixMilli()
	if !MsgIsScaleFence(msg) {
		msg.UpdateInjectTime(nowMs)
	}
}

func extractProduceToConsumeTime[KeyT, ValT any](msg *commtypes.Message, isInitialSrc bool, collector *stats.Int64Collector) {
	if !isInitialSrc {
		ts := msg.ExtractInjectTimeMs()
		dur := time.Now().UnixMilli() - ts
		collector.AddSample(dur)
	}
}
