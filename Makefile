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

fmt:
	@gofmt -tabs=false -tabwidth=2 -w .

.PHONY: help fmt
