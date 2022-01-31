module sharedlog-stream

go 1.17

require (
	cs.utexas.edu/zjia/faas v0.0.0
	github.com/confluentinc/confluent-kafka-go v1.7.0
	github.com/gammazero/deque v0.1.0
	github.com/rs/zerolog v1.19.0
	github.com/spaolacci/murmur3 v1.1.0
	github.com/stretchr/testify v1.7.0
	github.com/tinylib/msgp v1.1.6
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c
	golang.org/x/time v0.0.0-20211116232009-f0f3c7e86c11
	golang.org/x/tools v0.1.7
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/igrmk/treemap v1.0.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/niemeyer/pretty v0.0.0-20200227124842-a10e7caefd8e // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/mod v0.5.0 // indirect
	golang.org/x/sys v0.0.0-20211013075003-97ac67df715c // indirect
	gopkg.in/check.v1 v1.0.0-20200227125254-8fa46927fb4f // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace cs.utexas.edu/zjia/faas => ../boki/worker/golang

replace cs.utexas.edu/zjia/faas/slib => ../boki/faas/slib
