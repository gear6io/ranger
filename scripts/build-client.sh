#!/bin/bash
set -e

echo "Building icebox-client..."
go build -o bin/icebox-client cmd/icebox-client/main.go
echo "✅ icebox-client built successfully"
