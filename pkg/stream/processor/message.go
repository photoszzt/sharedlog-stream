package processor

type Message struct {
	Key       interface{}
	Value     interface{}
	Timestamp uint64
}

var EmptyMessage = Message{}
