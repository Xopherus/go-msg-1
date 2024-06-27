all: lint test

fmt:
	golangci-lint run ./... --fix

lint:
	golangci-lint run ./...

test:
	go test -race -v ./...

.PHONY: lint test fmt
