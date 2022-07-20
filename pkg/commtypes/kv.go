package commtypes

import (
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type CTXID struct{}

type CreateChangelogManagerParam struct {
	Env           types.Environment
	NumPartition  uint8
	FlushDuration time.Duration
	TimeOut       time.Duration
}
