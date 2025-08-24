# Ranger Makefile
# Build and run targets for Ranger server and client

# Variables
BINARY_DIR = bin
SERVER_BINARY = $(BINARY_DIR)/ranger-server
CLIENT_BINARY = $(BINARY_DIR)/ranger-client

# Go build flags
GO_BUILD_FLAGS = -ldflags="-s -w"
GO_VERSION = $(shell go version | awk '{print $$3}')

# Default target
.DEFAULT_GOAL := help

# Help target
.PHONY: help
help: ## Show this help message
	@echo "Ranger Makefile - Available targets:"
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
	@echo "  make fmt-all         # Format all code files"
	@echo "  make install-formatting-tools # Install formatting tools"
	@echo "  make install-hooks       # Install git pre-commit and commit-msg hooks"
	@echo "  make install-pre-commit-framework # Install pre-commit framework hooks"
	@echo "  make uninstall-pre-commit-framework # Uninstall pre-commit framework hooks"
	@echo "  make pre-commit          # Run pre-commit checks"
	@echo "  make release-build       # Build release binaries"
	@echo "  make release-clean       # Clean release binaries"
	@echo "  make status              # Show build status"

# Build targets
.PHONY: build-server
build-server: ## Build the Ranger server
	@echo "Building Ranger server..."
	@go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY) ./cmd/ranger-server/main.go
	@echo "✅ Server built successfully: $(SERVER_BINARY)"

.PHONY: build-client
build-client: ## Build the Ranger client
	@echo "Building Ranger client..."
	@go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY) ./cmd/ranger-client/main.go
	@echo "✅ Client built successfully: $(CLIENT_BINARY)"

.PHONY: build-all
build-all: build-server build-client ## Build server and client
	@echo "🎉 All binaries built successfully!"

# Development targets
.PHONY: dev-server
dev-server: build-server ## Build and run server in development mode
	@echo "Starting Ranger server in development mode..."
	@if [ -f "ranger-server-dev.yml" ]; then \
		./bin/ranger-server --config ranger-server-dev.yml; \
	else \
		./bin/ranger-server --config ranger-server.yml; \
	fi

.PHONY: run-server
run-server: ## Run the server (assumes it's already built)
	@if [ ! -f "$(SERVER_BINARY)" ]; then \
		echo "❌ Server binary not found. Run 'make build-server' first."; \
		exit 1; \
	fi
	@echo "Starting Ranger server..."
	@./$(SERVER_BINARY)

.PHONY: run-server-debug
run-server-debug: ## Run the server with debug logging
	@if [ ! -f "$(SERVER_BINARY)" ]; then \
		echo "❌ Server binary not found. Run 'make build-server' first."; \
		exit 1; \
	fi
	@echo "Starting Ranger server with debug logging..."
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
	@docker build -f Dockerfile.server -t ranger-server .

.PHONY: docker-build-client
docker-build-client: ## Build client Docker image
	@echo "Building client Docker image..."
	@docker build -f Dockerfile.client -t ranger-client .

.PHONY: docker-build
docker-build: docker-build-server docker-build-client ## Build all Docker images

.PHONY: docker-run
docker-run: ## Run with Docker Compose
	@echo "Starting Ranger with Docker Compose..."
	@docker-compose up

.PHONY: docker-run-detached
docker-run-detached: ## Run with Docker Compose in detached mode
	@echo "Starting Ranger with Docker Compose (detached)..."
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
kill-server: ## Kill running ranger server processes
	@echo "Killing running ranger server processes..."
	@if pkill -f ranger-server; then \
		echo "✅ Ranger server processes killed successfully"; \
	else \
		echo "ℹ️  No running ranger server processes found"; \
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

.PHONY: fmt-all
fmt-all: ## Format all code files (Go, Python, JS, etc.)
	@echo "Formatting all code files..."
	@echo "📝 Formatting Go code..."
	@go fmt ./...
	@echo "📝 Formatting Go imports..."
	@if command -v goimports &> /dev/null; then \
		find . -name "*.go" -not -path "./vendor/*" -not -path "./.git/*" -exec goimports -w {} \;; \
	else \
		echo "⚠️  goimports not found, install with: go install golang.org/x/tools/cmd/goimports@latest"; \
	fi
	@echo "📝 Formatting Markdown..."
	@if command -v markdownlint &> /dev/null; then \
		find . -name "*.md" -not -path "./vendor/*" -not -path "./.git/*" -exec markdownlint --fix {} \;; \
	else \
		echo "⚠️  markdownlint not found, install with: npm install -g markdownlint-cli"; \
	fi
	@echo "✅ All files formatted"

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

.PHONY: check-errorcodes
check-errorcodes: ## Check for unused ErrorCodes and forbidden error patterns in server packages
	@echo "Checking ErrorCodes and error patterns in server packages..."
	@cd scripts/linters/errorcode-checker && $(MAKE) check-server

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
	@echo "Ranger Build Status:"
	@echo "==================="
	@echo "Go version: $(GO_VERSION)"
	@echo ""
	@echo "Binaries:"
	@if [ -f "$(SERVER_BINARY)" ]; then echo "✅ Server binary: $(SERVER_BINARY)"; else echo "❌ Server binary: Missing"; fi
	@if [ -f "$(CLIENT_BINARY)" ]; then echo "✅ Client binary: $(CLIENT_BINARY)"; else echo "❌ Client binary: Missing"; fi
	@echo ""
	@echo "Configuration:"
	@if [ -f "ranger-server.yml" ]; then echo "✅ Server config: ranger-server.yml"; else echo "❌ Server config: Missing"; fi
	@if [ -f "ranger-client.yml" ]; then echo "✅ Client config: ranger-client.yml"; else echo "❌ Client config: Missing"; fi

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

.PHONY: install-pre-commit-framework
install-pre-commit-framework: ## Install pre-commit framework hooks
	@echo "Installing pre-commit framework hooks..."
	@if ! command -v pre-commit &> /dev/null; then \
		echo "Installing pre-commit framework..."; \
		pip install pre-commit; \
	fi
	@pre-commit install
	@echo "✅ Pre-commit framework hooks installed"

.PHONY: uninstall-pre-commit-framework
uninstall-pre-commit-framework: ## Uninstall pre-commit framework hooks
	@echo "Uninstalling pre-commit framework hooks..."
	@if command -v pre-commit &> /dev/null; then \
		pre-commit uninstall; \
		echo "✅ Pre-commit framework hooks uninstalled"; \
	else \
		echo "⚠️  pre-commit framework not found"; \
	fi

.PHONY: pre-commit
pre-commit: fmt-all vet check-errorcodes mod-tidy mod-verify deps ## Run pre-commit checks
	@echo "✅ Pre-commit checks passed"

# Release targets
.PHONY: release-build
release-build: clean ## Build release binaries
	@echo "Building release binaries..."
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY)-linux-amd64 ./cmd/ranger-server/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY)-darwin-amd64 ./cmd/ranger-server/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(GO_BUILD_FLAGS) -o $(SERVER_BINARY)-darwin-arm64 ./cmd/ranger-server/main.go
	@CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY)-linux-amd64 ./cmd/ranger-client/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY)-darwin-amd64 ./cmd/ranger-client/main.go
	@CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build $(GO_BUILD_FLAGS) -o $(CLIENT_BINARY)-darwin-arm64 ./cmd/ranger-client/main.go
	@echo "✅ Release binaries built"

.PHONY: release-clean
release-clean: ## Clean release binaries
	@echo "Cleaning release binaries..."
	@rm -f $(SERVER_BINARY)-* $(CLIENT_BINARY)-*
	@echo "✅ Release binaries cleaned" 