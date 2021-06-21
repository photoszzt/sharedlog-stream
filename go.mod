module cs.utexas.edu/zhitingz/sharedlog-stream

go 1.14

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/bradfitz/gomemcache v0.0.0-20190913173617-a41fca850d0b
	github.com/golang/protobuf v1.5.2
	github.com/rs/zerolog v1.19.0
	github.com/tatsuhiro-t/go-nghttp2 v0.0.0-20150408091349-4742878d9c90
	golang.org/x/net v0.0.0-20190620200207-3b0461eec859
)

replace cs.utexas.edu/zjia/faas => ./deps/faas/worker/golang

replace cs.utexas.edu/zjia/faas/slib => ./deps/faas/slib
