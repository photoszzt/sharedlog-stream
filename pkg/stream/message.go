package stream

type Message struct {
	Key   interface{}
	Value interface{}
}

var EmptyMessage = Message{}
