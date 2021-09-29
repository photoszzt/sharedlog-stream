package processor

type LogStore interface {
	Push(payload []byte, parNum uint32) (uint64, error)
	Pop(parNum uint32) ([]byte /* payload */, error)
	// PopBlocking() ([]byte /* payload */, error)
	TopicName() string
}
