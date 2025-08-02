#!/bin/bash
set -e

echo "Setting up development environment..."

# Create necessary directories
mkdir -p data
mkdir -p logs
mkdir -p bin

# Set up development configuration
if [ ! -f "icebox-server-dev.yml" ]; then
    cat > icebox-server-dev.yml << 'DEVEOF'
version: "0.1.0"

http:
  enabled: true
  address: "0.0.0.0"
  port: 8080
  cors:
    enabled: true
    allow_origins: ["*"]

jdbc:
  enabled: true
  address: "0.0.0.0"
  port: 5432
  max_connections: 10

storage:
  type: "filesystem"
  filesystem:
    root_path: "./data"
  catalog:
    type: "sqlite"
    sqlite:
      path: "./data/catalog.db"

logging:
  level: "debug"
  format: "text"
DEVEOF
fi

if [ ! -f "icebox-client-dev.yml" ]; then
    cat > icebox-client-dev.yml << 'DEVEOF'
server:
  address: "localhost:8080"
  timeout: 30s

database:
  name: "default"

logging:
  level: "debug"
  format: "text"
DEVEOF
fi

echo "✅ Development environment setup complete"
echo "📝 Configuration files created:"
echo "   - icebox-server-dev.yml"
echo "   - icebox-client-dev.yml"
echo ""
echo "🚀 To start development:"
echo "   1. ./scripts/build.sh"
echo "   2. ./bin/icebox-server --config icebox-server-dev.yml"
echo "   3. ./bin/icebox-client --config icebox-client-dev.yml shell"
