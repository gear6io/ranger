#!/bin/bash

# Icebox Restructuring Script
# This script helps migrate from the current monolithic architecture to the new client-server architecture

set -e

echo "🧊 Icebox Architecture Restructuring Script"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "go.mod" ] || [ ! -f "README.md" ]; then
    print_error "This script must be run from the icebox root directory"
    exit 1
fi

print_status "Starting icebox architecture restructuring..."

# Create new directory structure
print_status "Creating new directory structure..."

# Server directories
mkdir -p server/config
mkdir -p server/protocols/http
mkdir -p server/protocols/jdbc
mkdir -p server/storage
mkdir -p server/query

# Client directories
mkdir -p client/config
mkdir -p client/protocols/http
mkdir -p client/protocols/jdbc
mkdir -p client/shell
mkdir -p client/commands

# Command directories
mkdir -p cmd/icebox-server
mkdir -p cmd/icebox-client

print_success "Directory structure created"

# Move existing server-related code
print_status "Moving server-related code..."

# Move JDBC implementation
if [ -f "engine/jdbc/protocol.go" ]; then
    cp engine/jdbc/protocol.go server/protocols/jdbc/
    cp engine/jdbc/handler.go server/protocols/jdbc/
    print_success "Moved JDBC protocol implementation"
fi

# Move HTTP server code
if [ -f "cli/serve.go" ]; then
    # Extract HTTP server parts (this would need manual work)
    print_warning "HTTP server code needs manual extraction from cli/serve.go"
fi

# Move storage-related code
if [ -d "fs" ]; then
    cp -r fs/* server/storage/
    print_success "Moved filesystem storage code"
fi

# Move catalog code
if [ -d "catalog" ]; then
    cp -r catalog/* server/storage/
    print_success "Moved catalog code"
fi

# Move engine code
if [ -d "engine" ]; then
    cp -r engine/* server/query/
    print_success "Moved query engine code"
fi

print_success "Server code migration completed"

# Create configuration files
print_status "Creating configuration files..."

# Create icebox-server.yml
cat > icebox-server.yml << 'EOF'
version: "0.1.0"

# HTTP Server Configuration
# All settings are now fixed and non-configurable
# Port: 2847, Address: 0.0.0.0, Enabled: true

# JDBC Server Configuration
# All settings are now fixed and non-configurable
# Port: 2848, Address: 0.0.0.0, Enabled: true

# Logging Configuration
log:
  level: "info"
  format: "console"
  file_path: "logs/icebox-server.log"
  console: true
  max_size: 100
  max_backups: 3
  max_age: 7
  cleanup: true
EOF

# Client config
cat > icebox-client.yml << 'EOF'
server:
  address: "localhost:2847"
  timeout: 30s

auth:
  username: ""
  password: ""

database:
  name: "default"

ssl:
  mode: "disable"
  cert_file: ""
  key_file: ""

logging:
  level: "info"
  format: "text"
EOF

print_success "Configuration files created"

# Create build scripts
print_status "Creating build scripts..."

# Server build script
cat > scripts/build-server.sh << 'EOF'
#!/bin/bash
set -e

echo "Building icebox-server..."
go build -o bin/icebox-server cmd/icebox-server/main.go
echo "✅ icebox-server built successfully"
EOF

# Client build script
cat > scripts/build-client.sh << 'EOF'
#!/bin/bash
set -e

echo "Building icebox-client..."
go build -o bin/icebox-client cmd/icebox-client/main.go
echo "✅ icebox-client built successfully"
EOF

# Combined build script
cat > scripts/build.sh << 'EOF'
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
EOF

chmod +x scripts/build-server.sh
chmod +x scripts/build-client.sh
chmod +x scripts/build.sh

print_success "Build scripts created"

# Create Docker files
print_status "Creating Docker configuration..."

# Server Dockerfile
cat > Dockerfile.server << 'EOF'
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .

# Build server
RUN go build -o icebox-server cmd/icebox-server/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/icebox-server .
COPY --from=builder /app/icebox-server.yml .

EXPOSE 8080 5432

CMD ["./icebox-server"]
EOF

# Client Dockerfile
cat > Dockerfile.client << 'EOF'
FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .

# Build client
RUN go build -o icebox-client cmd/icebox-client/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates

WORKDIR /root/
COPY --from=builder /app/icebox-client .
COPY --from=builder /app/icebox-client.yml .

CMD ["./icebox-client", "--help"]
EOF

# Docker Compose
cat > docker-compose.yml << 'EOF'
version: '3.8'

services:
  icebox-server:
    build:
      context: .
      dockerfile: Dockerfile.server
    ports:
      - "8080:8080"
      - "5432:5432"
    volumes:
      - ./data:/root/data
      - ./icebox-server.yml:/root/icebox-server.yml
    environment:
      - ICEBOX_ENV=production
    restart: unless-stopped

  icebox-client:
    build:
      context: .
      dockerfile: Dockerfile.client
    depends_on:
      - icebox-server
    volumes:
      - ./icebox-client.yml:/root/icebox-client.yml
    command: ["./icebox-client", "shell"]
EOF

print_success "Docker configuration created"

# Create migration guide
print_status "Creating migration guide..."

cat > MIGRATION.md << 'EOF'
# Migration Guide: Monolithic to Client-Server Architecture

## Overview

This guide helps you migrate from the current monolithic icebox CLI to the new client-server architecture.

## What's Changed

### Before (Monolithic)
```bash
./icebox serve --port 8080
./icebox sql "SELECT * FROM table"
./icebox import data.parquet --table sales
```

### After (Client-Server)
```bash
# Terminal 1: Start server
./icebox-server

# Terminal 2: Use client
./icebox-client query "SELECT * FROM table"
./icebox-client import data.parquet --table sales
```

## Migration Steps

### 1. Build New Components

```bash
# Build both server and client
./scripts/build.sh

# Or build individually
./scripts/build-server.sh
./scripts/build-client.sh
```

### 2. Start Server

```bash
# Using default configuration
./bin/icebox-server

# Using custom configuration
./bin/icebox-server --config icebox-server.yml
```

### 3. Use Client

```bash
# Execute queries
./bin/icebox-client query "SELECT 1"

# Interactive shell
./bin/icebox-client shell

# Import data
./bin/icebox-client import data.parquet --table sales
```

### 4. Docker Deployment

```bash
# Start server and client
docker-compose up -d

# Use client
docker-compose exec icebox-client ./icebox-client shell
```

## Configuration Migration

### Old Configuration
The old `.icebox.yml` configuration is still supported for backward compatibility.

### New Configuration

#### Server Configuration (`icebox-server.yml`)
```yaml
version: "0.1.0"
http:
  enabled: true
  port: 8080
jdbc:
  enabled: true
  port: 5432
storage:
  type: "filesystem"
  filesystem:
    root_path: "./data"
```

#### Client Configuration (`icebox-client.yml`)
```yaml
server:
  address: "localhost:8080"
  timeout: 30s
database:
  name: "default"
```

## Backward Compatibility

The old monolithic CLI (`./icebox`) will continue to work during the transition period. You can:

1. Use the old CLI for existing workflows
2. Gradually migrate to the new client-server architecture
3. Run both simultaneously for testing

## Testing

### Test Server
```bash
# Start server
./bin/icebox-server

# Test HTTP API
curl http://localhost:8080/health

# Test JDBC (if you have a PostgreSQL client)
psql -h localhost -p 5432 -U test -d icebox
```

### Test Client
```bash
# Test basic functionality
./bin/icebox-client query "SELECT 1 as test"

# Test shell
./bin/icebox-client shell
```

## Troubleshooting

### Common Issues

1. **Port already in use**: Change ports in configuration
2. **Permission denied**: Ensure proper file permissions
3. **Connection refused**: Check if server is running

### Logs

- Server logs: Check console output or log files
- Client logs: Use `--verbose` flag for detailed output

## Next Steps

1. Test the new architecture thoroughly
2. Update deployment scripts
3. Update documentation
4. Plan production deployment
5. Deprecate old monolithic CLI (future release)
EOF

print_success "Migration guide created"

# Create development setup script
print_status "Creating development setup..."

cat > scripts/dev-setup.sh << 'EOF'
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
EOF

chmod +x scripts/dev-setup.sh

print_success "Development setup script created"

# Final summary
echo ""
echo "🎉 Restructuring completed successfully!"
echo "======================================"
echo ""
echo "📁 New structure created:"
echo "   ├── server/           # Server implementation"
echo "   ├── client/           # Client implementation"
echo "   ├── cmd/icebox-server/ # Server entry point"
echo "   ├── cmd/icebox-client/ # Client entry point"
echo "   └── scripts/          # Build and setup scripts"
echo ""
echo "📄 Configuration files:"
echo "   ├── icebox-server.yml # Server configuration"
echo "   ├── icebox-client.yml # Client configuration"
echo "   └── MIGRATION.md      # Migration guide"
echo ""
echo "🐳 Docker support:"
echo "   ├── Dockerfile.server # Server container"
echo "   ├── Dockerfile.client # Client container"
echo "   └── docker-compose.yml # Development setup"
echo ""
echo "🚀 Next steps:"
echo "   1. Review the new structure"
echo "   2. Complete the code migration (manual work required)"
echo "   3. Test the new architecture"
echo "   4. Update documentation"
echo "   5. Deploy to production"
echo ""
echo "📖 See MIGRATION.md for detailed migration instructions"
echo "🔧 Run ./scripts/dev-setup.sh to set up development environment" 