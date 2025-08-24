#!/bin/bash
set -e

echo "Building ranger-client..."
go build -o bin/ranger-client cmd/ranger-client/main.go
echo "✅ ranger-client built successfully"
