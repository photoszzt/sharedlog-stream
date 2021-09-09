package processor

type Message struct {
	Key   interface{}
	Value interface{}
}

type MessageSerialized struct {
	Key   []byte `json:"k,omitempty"`
	Value []byte `json:"v"`
}

var EmptyMessage = Message{}
