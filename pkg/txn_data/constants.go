package txn_data

import "math"

const (
	LogTagReserveBits = 3

	PartitionBits = 8
)

const (
	TransactionLogBegin = iota
	TransactionLogFence
	TxnMarkLowBits
	ScaleFenceLowBits
)

const (
	SCALE_FENCE_KEY = "__scale_fence"
)

func BeginTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + TransactionLogBegin
}

func FenceTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + TransactionLogFence
}

func ScaleFenceTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + ScaleFenceLowBits
}
