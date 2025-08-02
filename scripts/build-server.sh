#!/bin/bash
set -e

echo "Building icebox-server..."
go build -o bin/icebox-server cmd/icebox-server/main.go
echo "✅ icebox-server built successfully"
