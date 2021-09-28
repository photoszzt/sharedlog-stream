package processor

type WindowedKey struct {
	Key    interface{}
	Window Window
}
