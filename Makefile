# norca-platform
GO      ?= go
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -ldflags "-X main.version=$(VERSION)"

.PHONY: all build test test-race lint vuln fmt help

all: fmt lint test build

## build: Build the programs into bin/
build:
	$(GO) build $(LDFLAGS) -o bin/ ./cmd/...

## test: Run the tests
test:
	$(GO) test -count=1 ./...

## test-race: Run the tests with the race detector (needs cgo; CI runs it on Linux)
test-race:
	$(GO) test -race -shuffle=on -count=1 ./...

## lint: Run golangci-lint v2 with .golangci.yml
lint:
	golangci-lint run ./...

## vuln: Check the module with govulncheck
vuln:
	govulncheck ./...

## fmt: Format with the gofmt of the toolchain in go.mod
fmt:
	"$$($(GO) env GOROOT)/bin/gofmt" -w .

## help: List the targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/^## //'
