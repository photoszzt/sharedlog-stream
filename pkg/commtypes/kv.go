package commtypes

import "cs.utexas.edu/zjia/faas/types"

type CTXID string

type KeyT interface{}

type ValueT interface{}

type CreateStreamParam struct {
	Env          types.Environment
	NumPartition uint8
}
