GO_FILES?=$$(find . -name '*.go' |grep -v deps)

default: goimports nexmark nexmark_debug nexmark_client nexmark_genevents_kafka dspbench dspbench_debug dspbench_client wordcount_client wordcount_genevents_kafka append_read_client

.PHONY: staticcheck
staticcheck:
	@staticcheck ./...
	@staticcheck ./... | grep -v deps ; if [ $$? -eq 1 ]; then \
		echo ""; \
		echo "Vet found suspicious constructs. Please check the reported constructs"; \
		echo "and fix them if necessary before submitting the code for review."; \
		exit 1; \
	fi

.PHONY: goimports
goimports:
	@go install golang.org/x/tools/cmd/goimports
	goimports -w $(GO_FILES)

.PHONY: nexmark
nexmark:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_handler ./benchmark/nexmark/nexmark_handler/nexmark_handler.go

.PHONY: nexmark_debug
nexmark_debug:
	mkdir -p ./bin
	GO111MODULE=on go build -tags debug -o bin/nexmark_handler_debug ./benchmark/nexmark/nexmark_handler/nexmark_handler.go

.PHONY: nexmark_client
nexmark_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_client ./benchmark/nexmark/nexmark_client

.PHONY: nexmark_genevents_kafka
nexmark_genevents_kafka:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/nexmark_genevents_kafka benchmark/nexmark/nexmark_genevents_kafka/nexmark_genevents_kafka.go

.PHONY: wordcount_genevents_kafka
wordcount_genevents_kafka:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/wordcount_genevents_kafka benchmark/dspbench/wordcount_genevents_kafka/wordcount_genevents_kafka.go

.PHONY: dspbench
dspbench:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/dspbench_handler benchmark/dspbench/dspbench_handler/dspbench_handler.go

.PHONY: dspbench_debug
dspbench_debug:
	mkdir -p ./bin
	GO111MODULE=on go build -tags debug -o bin/dspbench_handler_debug benchmark/dspbench/dspbench_handler/dspbench_handler.go

.PHONY: dspbench_client
dspbench_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/dspbench_client benchmark/dspbench/dspbench_client/dspbench_client.go

.PHONY: wordcount_client
wordcount_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/wordcount_client benchmark/dspbench/wordcount_client/wordcount_client.go

.PHONY: append_read_client
append_read_client:
	mkdir -p ./bin
	GO111MODULE=on go build -o bin/append_read_client benchmark/dspbench/append_read_client/append_read_client.go

.PHONY: download-book
download-book:
	mkdir -p data
	wget -O data/books.dat https://raw.githubusercontent.com/mayconbordin/storm-applications/master/data/books.dat
