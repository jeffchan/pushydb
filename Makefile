GOPATH := ${PWD}
export GOPATH=${PWD}

define USAGE
Usage instructions:
    make fmt                  runs gofmt on the source code
    make test                 runs the tests
    make help                 displays this message
endef
export USAGE

default: help

help:
	@echo "$$USAGE"

get:
	@go get shardkv

fmt:
	@gofmt -tabs=false -tabwidth=2 -w .

test: fmt get
	@go test shardkv

.PHONY: help fmt test get
