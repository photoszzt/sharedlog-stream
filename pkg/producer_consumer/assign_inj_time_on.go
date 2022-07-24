//go:build stats
// +build stats

package producer_consumer

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
	"sharedlog-stream/pkg/txn_data"
	"time"
)

func MsgIsScaleFence(msg *commtypes.Message) bool {
	ctrl, ok := msg.Key.(string)
	return ok && ctrl == txn_data.SCALE_FENCE_KEY
}

func assignInjTime(msg *commtypes.Message) {
	nowMs := time.Now().UnixMilli()
	if !MsgIsScaleFence(msg) {
		msg.UpdateInjectTime(nowMs)
	}
}

func extractProduceToConsumeTime(msg *commtypes.Message, isInitialSrc bool, collector *stats.Int64Collector) {
	if !isInitialSrc {
		ts := msg.ExtractInjectTimeMs()
		dur := time.Now().UnixMilli() - ts
		collector.AddSample(dur)
	}
}
