//go:build !stats
// +build !stats

package source_sink

import "sharedlog-stream/pkg/stream/processor/commtypes"

func assignInjTime(msg *commtypes.Message) {}
