package commtypes

import (
	"time"
)

type CTXID struct{}

type KeyT interface{}

type ValueT interface{}

type CreateChangelogManagerParam struct {
	NumPartition  uint8
	FlushDuration time.Duration
	TimeOut       time.Duration
}

type ENVID struct{}
