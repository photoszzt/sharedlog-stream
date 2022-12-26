package txn_data

import (
	"math"
)

const (
	LogTagReserveBits = 3

	PartitionBits = 8
)

const (
	Begin = iota
	Fence
	MarkLowBits
	ScaleFenceLowBits
	CtrlMetaLowBits
	AbortLowBits
	CommitLowBits
)

const (
	SCALE_FENCE_KEY = "__scale_fence"
)

func BeginTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + Begin
}

func FenceTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + Fence
}

func ScaleFenceTag(nameHash uint64, parNum uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(parNum)<<LogTagReserveBits + ScaleFenceLowBits
}

func MarkerTag(nameHash uint64, par uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(par)<<LogTagReserveBits + MarkLowBits
}

func CtrlMetaTag(nameHash uint64, par uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(par)<<LogTagReserveBits + CtrlMetaLowBits
}

func TxnAbortTag(nameHash uint64, par uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(par)<<LogTagReserveBits + AbortLowBits
}

func TxnCommitTag(nameHash uint64, par uint8) uint64 {
	mask := uint64(math.MaxUint64) - (1<<(PartitionBits+LogTagReserveBits) - 1)
	return nameHash&mask + uint64(par)<<LogTagReserveBits + CommitLowBits
}
