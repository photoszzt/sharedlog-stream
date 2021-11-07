package store

type Stream interface {
	Push(payload []byte, parNum uint8, additionalTag []uint64) (uint64, error)
	ReadNext(parNum uint8) ([]byte /* payload */, error)
	// PopBlocking() ([]byte /* payload */, error)
	TopicName() string
}
