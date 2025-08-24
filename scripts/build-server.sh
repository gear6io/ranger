#!/bin/bash
set -e

echo "Building ranger-server..."
go build -o bin/ranger-server cmd/ranger-server/main.go
echo "✅ ranger-server built successfully"
