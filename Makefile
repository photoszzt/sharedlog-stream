GO_FILES?=$$(find . -name '*.go' |grep -v deps)

default: goimports nexmark

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
	@go get golang.org/x/tools/cmd/goimports
	goimports -w $(GO_FILES)

.PHONY: nexmark
nexmark:
	mkdir -p ./bin
	go build -o bin/nexmark cmd/nexmark/nexmark.go
