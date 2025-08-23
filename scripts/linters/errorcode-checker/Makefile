# Makefile for ErrorCode checker tool

.PHONY: build run test clean help check-server check-client check-pkg check-all

# Default target
.DEFAULT_GOAL := help

help: ## Show this help message
	@echo "ErrorCode Checker - Available targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Examples:"
	@echo "  make check-server    # Check server directory only"
	@echo "  make check-client    # Check client directory only"
	@echo "  make check-pkg       # Check pkg directory only"
	@echo "  make check-all       # Check all directories"

check-server: ## Check only server directory
	@echo "Checking server directory..."
	@go run . -dir ../../../server -config .errorcode.yml

check-client: ## Check only client directory
	@echo "Checking client directory..."
	@go run . -dir ../../../client -config .errorcode.yml

check-pkg: ## Check only pkg directory
	@echo "Checking pkg directory..."
	@go run . -dir ../../../pkg -config .errorcode.yml

check-all: ## Check all main directories
	@echo "Checking all directories..."
	@go run . -dir ../../../ -config .errorcode.yml

test: ## Run tests
	@echo "Running tests..."
	@go test -v .

clean: ## Clean build artifacts (no binary to clean)
	@echo "No build artifacts to clean"

build: ## Build the tool (optional, uses go run by default)
	@echo "Building ErrorCode checker..."
	@go build -o ../../../bin/errorcode-checker .
	@echo "✅ Built successfully: ../../../bin/errorcode-checker"
