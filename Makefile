.PHONY: help deps build check vet lint fmt unit-test unit-test-coverage

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

deps: ## Tidy and download dependencies
	go mod tidy
	go mod download

build: ## Build all packages
	go build ./...

build-lib: ## Build library only (excludes examples)
	go build .

check: build ## Build application and test code (compile tests without running)
	go test -run=^$$ -count=1 ./... > /dev/null

vet: ## Run go vet
	go vet ./...

lint: vet ## Run golangci-lint (falls back to go vet if linter not installed)
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		echo "golangci-lint not installed, skipping (go vet already ran)"; \
	fi

fmt: ## Run gofmt and check for differences
	@gofmt -l . | tee /dev/stderr | (! read) || (echo "gofmt found unformatted files" && exit 1)

unit-test: ## Run unit tests with race detector
	go test -v -race -count=5 ./... > /tmp/test_output.log 2>&1

unit-test-coverage: ## Run unit tests with coverage and race detector
	go test -v -race -count=1 -coverprofile=/tmp/coverage.out ./... > /tmp/test_output.log 2>&1
	@go tool cover -func=/tmp/coverage.out | tail -1
