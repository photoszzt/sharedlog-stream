package processor

type Source interface {
	// Consume gets the next Message from the source
	Consume() (Message, error)
}
