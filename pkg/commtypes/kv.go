package commtypes

import (
	"time"

	"cs.utexas.edu/zjia/faas/types"
)

type CTXID string

type KeyT interface{}

type ValueT interface{}

type CreateChangelogManagerParam struct {
	Env           types.Environment
	NumPartition  uint8
	FlushDuration time.Duration
	TimeOut       time.Duration
}
