GO_FILES?=$$(find . -name '*.go' |grep -v deps)

default: goimports nexmark nexmark_client nexmark_genevents_kafka

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
	go build -o bin/nexmark benchmark/nexmark/nexmark.go

.PHONY: nexmark_client
nexmark_client:
	mkdir -p ./bin
	go build -o bin/nexmark_client benchmark/nexmark_client/nexmark_client.go

.PHONY: nexmark_genevents_kafka
nexmark_genevents_kafka:
	mkdir -p ./bin
	go build -o bin/nexmark_genevents_kafka benchmark/nexmark_genevents_kafka/nexmark_genevents_kafka.go
