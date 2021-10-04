module sharedlog-stream

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/gammazero/deque v0.1.0
	github.com/rs/zerolog v1.19.0
	github.com/tinylib/msgp v1.1.6
	golang.org/x/tools v0.0.0-20201022035929-9cf592e881e9 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

replace cs.utexas.edu/zjia/faas => ../faas/worker/golang

replace cs.utexas.edu/zjia/faas/slib => ../deps/faas/slib
