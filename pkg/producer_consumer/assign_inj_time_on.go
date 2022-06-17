//go:build stats
// +build stats

package producer_consumer

import (
	"sharedlog-stream/pkg/commtypes"
	"sharedlog-stream/pkg/stats"
	"time"
)

func assignInjTime(msg *commtypes.Message) {
	nowMs := time.Now().UnixMilli()
	if !MsgIsScaleFence(msg) {
		commtypes.UpdateValInjectTime(msg, nowMs)
	}
}

func extractProduceToConsumeTime(msgSeqs *commtypes.MsgAndSeqs, isInitialSrc bool, collector *stats.Int64Collector) error {
	if !isInitialSrc {
		callback := func(msg *commtypes.Message) error {
			injExtractor, ok := msg.Value.(commtypes.InjectTimeGetterSetter)
			if ok {
				ts, err := injExtractor.ExtractInjectTimeMs()
				if err != nil {
					return err
				}
				dur := time.Now().UnixMilli() - ts
				collector.AddSample(dur)
			}
			return nil
		}
		err := commtypes.ApplyFuncToMsgSeqs(msgSeqs, callback)
		if err != nil {
			return err
		}
	}
	return nil
}
