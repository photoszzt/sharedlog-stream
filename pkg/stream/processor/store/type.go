//go:generate msgp
//msgp:ignore ValueTimestamp Change ValueTimestampJSONSerde ValueTimestampMsgpSerde
package store

type KeyT interface{}

type ValueT interface{}

type Change struct {
	OldValue interface{}
	NewValue interface{}
}
