# Icebox Makefile
# Build and run targets for Icebox server and client

# Variables
BINARY_DIR = .
SERVER_BINARY = $(BINARY_DIR)/icebox-server
CLIENT_BINARY = $(BINARY_DIR)/icebox-client

# Go build flags
GO_BUILD_FLAGS = -ldflags="-s -w"
GO_VERSION = $(shell go version | awk '{print $$3}')

# Default target
.DEFAULT_GOAL := help

# Help target
.PHONY: help
help: ## Show this help message
	@echo "Icebox Makefile - Available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make build-server    # Build only the server"
	@echo "  make build-client    # Build only the client"
	@echo "  make build-all       # Build server and client"
	@echo "  make run-server      # Run the server"
	@echo "  make kill-server     # Kill running server processes"
	@echo "  make clean           # Clean all binaries"

# Build targets
.PHONY: build-server
build-server: ## Build the Icebox server
	@echo "Building Icebox server..."
	@go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY) ./cmd/icebox-server/main.go
	@echo "✅ Server built successfully: $(SERVER_BINARY)"

.PHONY: build-client
build-client: ## Build the Icebox client
	@echo "Building Icebox client..."
	@go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY) ./cmd/icebox-client/main.go
	@echo "✅ Client built successfully: $(CLIENT_BINARY)"

.PHONY: build-all
build-all: build-server build-client ## Build server and client
	@echo "🎉 All binaries built successfully!"

# Development targets
.PHONY: dev-server
dev-server: build-server ## Build and run server in development mode
	@echo "Starting Icebox server in development mode..."
	@./$(SERVER_BINARY)

.PHONY: run-server
run-server: ## Run the server (assumes it's already built)
	@if [ ! -f "$(SERVER_BINARY)" ]; then \
		echo "❌ Server binary not found. Run 'make build-server' first."; \
		exit 1; \
	fi
	@echo "Starting Icebox server..."
	@./$(SERVER_BINARY)

.PHONY: run-server-debug
run-server-debug: ## Run the server with debug logging
	@if [ ! -f "$(SERVER_BINARY)" ]; then \
		echo "❌ Server binary not found. Run 'make build-server' first."; \
		exit 1; \
	fi
	@echo "Starting Icebox server with debug logging..."
	@LOG_LEVEL=debug ./$(SERVER_BINARY)

# Testing targets
.PHONY: test
test: ## Run all tests
	@echo "Running tests..."
	@go test -v ./...

.PHONY: test-server
test-server: ## Run server tests
	@echo "Running server tests..."
	@go test -v ./server/...

.PHONY: test-client
test-client: ## Run client tests
	@echo "Running client tests..."
	@go test -v ./client/...

.PHONY: test-integration
test-integration: ## Run integration tests
	@echo "Running integration tests..."
	@go test -v ./integration_tests/...

# Docker targets
.PHONY: docker-build-server
docker-build-server: ## Build server Docker image
	@echo "Building server Docker image..."
	@docker build -f Dockerfile.server -t icebox-server .

.PHONY: docker-build-client
docker-build-client: ## Build client Docker image
	@echo "Building client Docker image..."
	@docker build -f Dockerfile.client -t icebox-client .

.PHONY: docker-build
docker-build: docker-build-server docker-build-client ## Build all Docker images

.PHONY: docker-run
docker-run: ## Run with Docker Compose
	@echo "Starting Icebox with Docker Compose..."
	@docker-compose up

.PHONY: docker-run-detached
docker-run-detached: ## Run with Docker Compose in detached mode
	@echo "Starting Icebox with Docker Compose (detached)..."
	@docker-compose up -d

.PHONY: docker-stop
docker-stop: ## Stop Docker Compose services
	@echo "Stopping Docker Compose services..."
	@docker-compose down

# Utility targets
.PHONY: clean
clean: ## Clean all built binaries
	@echo "Cleaning binaries..."
	@rm -f $(SERVER_BINARY) $(CLIENT_BINARY)
	@echo "✅ Cleaned successfully"

.PHONY: kill-server
kill-server: ## Kill running icebox server processes
	@echo "Killing running icebox server processes..."
	@if pkill -f icebox-server; then \
		echo "✅ Icebox server processes killed successfully"; \
	else \
		echo "ℹ️  No running icebox server processes found"; \
	fi

.PHONY: clean-all
clean-all: clean ## Clean binaries and go cache
	@echo "Cleaning Go cache..."
	@go clean -cache -modcache -testcache
	@echo "✅ All cleaned successfully"

.PHONY: fmt
fmt: ## Format Go code
	@echo "Formatting Go code..."
	@go fmt ./...

.PHONY: vet
vet: ## Run go vet
	@echo "Running go vet..."
	@go vet ./...

.PHONY: lint
lint: ## Run linting (requires golangci-lint)
	@if ! command -v golangci-lint &> /dev/null; then \
		echo "❌ golangci-lint not found. Install with: go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
		exit 1; \
	fi
	@echo "Running golangci-lint..."
	@golangci-lint run

.PHONY: mod-tidy
mod-tidy: ## Tidy go.mod and go.sum
	@echo "Tidying go.mod and go.sum..."
	@go mod tidy
	@echo "✅ Go modules tidied"

.PHONY: mod-verify
mod-verify: ## Verify go.mod and go.sum
	@echo "Verifying go.mod and go.sum..."
	@go mod verify
	@echo "✅ Go modules verified"

.PHONY: deps
deps: ## Download dependencies
	@echo "Downloading dependencies..."
	@go mod download
	@echo "✅ Dependencies downloaded"

# Status targets
.PHONY: status
status: ## Show build status
	@echo "Icebox Build Status:"
	@echo "===================="
	@echo "Go version: $(GO_VERSION)"
	@echo ""
	@echo "Binaries:"
	@if [ -f "$(SERVER_BINARY)" ]; then echo "✅ Server: $(SERVER_BINARY)"; else echo "❌ Server: Not built"; fi
	@if [ -f "$(CLIENT_BINARY)" ]; then echo "✅ Client: $(CLIENT_BINARY)"; else echo "❌ Client: Not built"; fi
	@echo ""
	@echo "Configuration:"
	@if [ -f "icebox-server.yml" ]; then echo "✅ Server config: icebox-server.yml"; else echo "❌ Server config: Missing"; fi
	@if [ -f "icebox-client.yml" ]; then echo "✅ Client config: icebox-client.yml"; else echo "❌ Client config: Missing"; fi

# Quick start targets
.PHONY: quick-start
quick-start: build-all run-server ## Build everything and start server

# Development workflow
.PHONY: dev-setup
dev-setup: deps mod-tidy fmt vet ## Setup development environment
	@echo "✅ Development environment ready"

.PHONY: install-hooks
install-hooks: ## Install git pre-commit and commit-msg hooks
	@echo "Installing git pre-commit and commit-msg hooks..."
	@./scripts/install-hooks.sh

.PHONY: pre-commit
pre-commit: fmt vet test mod-tidy ## Run pre-commit checks
	@echo "✅ Pre-commit checks passed"

# Release targets
.PHONY: release-build
release-build: clean ## Build release binaries
	@echo "Building release binaries..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY)-linux-amd64 ./cmd/icebox-server/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY)-darwin-amd64 ./cmd/icebox-server/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY)-darwin-arm64 ./cmd/icebox-server/main.go
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY)-linux-amd64 ./cmd/icebox-client/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY)-darwin-amd64 ./cmd/icebox-client/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY)-darwin-arm64 ./cmd/icebox-client/main.go
	@echo "✅ Release binaries built"

.PHONY: release-clean
release-clean: ## Clean release binaries
	@echo "Cleaning release binaries..."
	@rm -f $(SERVER_BINARY)-* $(CLIENT_BINARY)-*
	@echo "✅ Release binaries cleaned" 