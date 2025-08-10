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

# HTTP Server Configuration
# All settings are now fixed and non-configurable
# Port: 2847, Address: 0.0.0.0, Enabled: true

# JDBC Server Configuration
# All settings are now fixed and non-configurable
# Port: 2848, Address: 0.0.0.0, Enabled: true

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
  address: "localhost:2847"
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
echo ""
echo "📡 Fixed Network Ports:"
echo "   - HTTP Server: 2847"
echo "   - JDBC Server: 2848"
echo "   - Native Server: 2849"
echo "   - MinIO Server: 2850"
echo "   - Health Check: 2851"
