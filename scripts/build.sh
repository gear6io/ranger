#!/bin/bash
set -e

echo "Building ranger components..."

# Create bin directory
mkdir -p bin

# Build server
echo "Building server..."
go build -o bin/ranger-server cmd/ranger-server/main.go

# Build client
echo "Building client..."
go build -o bin/ranger-client cmd/ranger-client/main.go

echo "✅ All components built successfully"
echo "📦 Binaries available in bin/ directory"
