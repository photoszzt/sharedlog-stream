package benchmarkframework

type Worker interface {
	InitDriver(configurationFile string)
	CreateTopics()
}
