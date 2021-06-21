package model

import (
	"math/rand"
)

func NextUint64(random rand.Rand, n uint64) uint64 {
	return random.Uint64()
}
