module cs.utexas.edu/zhitingz/sharedlog-stream

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/confluentinc/confluent-kafka-go v1.7.0 // indirect
	github.com/golang/protobuf v1.5.2
	github.com/rs/zerolog v1.19.0
	github.com/tatsuhiro-t/go-nghttp2 v0.0.0-20150408091349-4742878d9c90
	github.com/tinylib/msgp v1.1.6
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4
	golang.org/x/sys v0.0.0-20210630005230-0f9fa26af87c // indirect
	golang.org/x/tools v0.1.5 // indirect
)

replace cs.utexas.edu/zjia/faas => ./deps/faas/worker/golang

replace cs.utexas.edu/zjia/faas/slib => ./deps/faas/slib
