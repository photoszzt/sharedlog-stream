package processor

type KeyT interface{}

type ValueT interface{}

type ValueTimestamp struct {
	Timestamp uint64
	Value     interface{}
}
