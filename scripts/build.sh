#!/bin/bash
set -e

echo "Building icebox components..."

# Create bin directory
mkdir -p bin

# Build server
echo "Building server..."
go build -o bin/icebox-server cmd/icebox-server/main.go

# Build client
echo "Building client..."
go build -o bin/icebox-client cmd/icebox-client/main.go

echo "✅ All components built successfully"
echo "📦 Binaries available in bin/ directory"
