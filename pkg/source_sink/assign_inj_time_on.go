//go:build stats
// +build stats

package source_sink

import (
	"sharedlog-stream/pkg/stream/processor/commtypes"
	"time"
)

func assignInjTime(msg *commtypes.Message) {
	nowMs := time.Now().UnixMilli()
	if !MsgIsScaleFence(msg) {
		commtypes.UpdateValInjectTime(msg, nowMs)
	}
}
