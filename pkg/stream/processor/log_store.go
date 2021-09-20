package processor

type LogStore interface {
	Push(payload []byte) (uint64, error)
	Pop() ([]byte /* payload */, error)
	PopBlocking() ([]byte /* payload */, error)
	TopicName() string
}
