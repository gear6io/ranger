# Icebox Architecture

## Overview

Icebox follows a client-server architecture similar to ClickHouse, with clear separation between client and server components. This design provides flexibility, scalability, and ease of deployment.

## Architecture Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   icebox-client │    │  icebox-server  │    │   Storage Layer │
│                 │    │                 │    │                 │
│ • CLI Interface │◄──►│ • HTTP Server   │◄──►│ • Local FS      │
│ • JDBC Client   │    │ • JDBC Server   │    │ • S3/MinIO      │
│ • REST Client   │    │ • Query Engine  │    │ • Memory        │
│ • Shell         │    │ • Catalog Mgmt  │    │                 │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Server Architecture

### Core Components

#### 1. Server (`server/server.go`)
- Main server orchestrator
- Manages multiple protocol servers (HTTP, JDBC)
- Handles graceful shutdown
- Coordinates storage and query execution

#### 2. Configuration (`server/config/`)
- Server-specific configuration management
- YAML-based configuration files
- Environment variable support
- Default configuration generation

#### 3. Storage Manager (`server/storage/`)
- Abstracts storage backends
- Manages catalog and data storage
- Supports multiple storage types:
  - Local filesystem
  - S3-compatible storage
  - In-memory storage

#### 4. Protocol Servers

##### HTTP Server (`server/protocols/http/`)
- RESTful API endpoints
- JSON request/response format
- Authentication and authorization
- CORS support
- Metrics and health checks

##### JDBC Server (`server/protocols/jdbc/`)
- PostgreSQL wire protocol compatibility
- Connection pooling
- Prepared statements
- Transaction support
- Metadata discovery

### Server Directory Structure

```
server/
├── server.go                 # Main server implementation
├── config/
│   ├── config.go            # Configuration management
│   └── defaults.go          # Default configurations
├── protocols/
│   ├── http/
│   │   ├── server.go        # HTTP server implementation
│   │   ├── handlers.go      # HTTP request handlers
│   │   └── middleware.go    # HTTP middleware
│   └── jdbc/
│       ├── server.go        # JDBC server implementation
│       ├── protocol.go      # PostgreSQL wire protocol
│       └── handler.go       # JDBC connection handler
├── storage/
│   ├── manager.go           # Storage manager
│   ├── local.go             # Local filesystem storage
│   ├── s3.go                # S3-compatible storage
│   └── memory.go            # In-memory storage
└── query/
    ├── engine.go            # Query execution engine
    ├── optimizer.go         # Query optimization
    └── executor.go          # Query execution
```

## Client Architecture

### Core Components

#### 1. Client (`client/client.go`)
- Main client implementation
- Connection management
- Protocol abstraction
- Error handling

#### 2. Configuration (`client/config/`)
- Client-specific configuration
- Connection settings
- Authentication configuration
- Default settings

#### 3. Protocols

##### HTTP Client (`client/protocols/http/`)
- REST API client
- JSON request/response handling
- Authentication
- Retry logic

##### JDBC Client (`client/protocols/jdbc/`)
- PostgreSQL driver compatibility
- Connection pooling
- Prepared statements
- Transaction management

### Client Directory Structure

```
client/
├── client.go                # Main client implementation
├── config/
│   ├── config.go            # Client configuration
│   └── defaults.go          # Default settings
├── protocols/
│   ├── http/
│   │   ├── client.go        # HTTP client implementation
│   │   └── transport.go     # HTTP transport layer
│   └── jdbc/
│       ├── client.go        # JDBC client implementation
│       └── connection.go    # JDBC connection management
├── shell/
│   ├── shell.go             # Interactive shell
│   ├── completer.go         # Command completion
│   └── history.go           # Command history
└── commands/
    ├── query.go             # Query execution commands
    ├── import.go            # Data import commands
    ├── table.go             # Table management commands
    └── catalog.go           # Catalog management commands
```

## Command Line Interface

### Server Commands

```bash
# Start server with default configuration
icebox-server

# Start server with custom config
icebox-server --config /path/to/config.yml

# Start server with specific protocols
icebox-server --http-only
icebox-server --jdbc-only

# Development mode
icebox-server --dev
```

### Client Commands

```bash
# Execute SQL query
icebox-client query "SELECT * FROM my_table"

# Interactive shell
icebox-client shell

# Import data
icebox-client import data.parquet --table sales

# Table management
icebox-client table list
icebox-client table describe sales

# Catalog management
icebox-client catalog namespaces
icebox-client catalog create-namespace analytics
```

## Configuration

### Server Configuration (`icebox-server.yml`)

```yaml
version: "0.1.0"

http:
  enabled: true
  address: "0.0.0.0"
  port: 8080
  read_timeout: 30s
  write_timeout: 30s
  cors:
    enabled: true
    allow_origins: ["*"]

jdbc:
  enabled: true
  address: "0.0.0.0"
  port: 5432
  max_connections: 50
  connection_timeout: 30s
  query_timeout: 5m

storage:
  type: "filesystem"
  filesystem:
    root_path: "./data"
  catalog:
    type: "sqlite"
    sqlite:
      path: "./catalog.db"

logging:
  level: "info"
  format: "json"
```

### Client Configuration (`icebox-client.yml`)

```yaml
server:
  address: "localhost:8080"
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
```

## Deployment

### Single Server Deployment

```bash
# Build server
go build -o icebox-server cmd/icebox-server/main.go

# Build client
go build -o icebox-client cmd/icebox-client/main.go

# Start server
./icebox-server

# Use client
./icebox-client query "SELECT 1"
```

### Docker Deployment

```dockerfile
# Server Dockerfile
FROM golang:1.24-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o icebox-server cmd/icebox-server/main.go

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/icebox-server .
EXPOSE 8080 5432
CMD ["./icebox-server"]
```

### Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: icebox-server
spec:
  replicas: 3
  selector:
    matchLabels:
      app: icebox-server
  template:
    metadata:
      labels:
        app: icebox-server
    spec:
      containers:
      - name: icebox-server
        image: icebox/server:latest
        ports:
        - containerPort: 8080
        - containerPort: 5432
        volumeMounts:
        - name: data
          mountPath: /data
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: icebox-data
```

## Benefits of This Architecture

### 1. Separation of Concerns
- Clear separation between client and server
- Independent development and deployment
- Protocol-specific optimizations

### 2. Scalability
- Multiple server instances
- Load balancing support
- Horizontal scaling

### 3. Flexibility
- Multiple client types (CLI, JDBC, REST)
- Multiple storage backends
- Configurable protocols

### 4. Maintainability
- Modular code structure
- Clear interfaces
- Comprehensive testing

### 5. Production Ready
- Graceful shutdown
- Health checks
- Metrics and monitoring
- Security features

## Migration from Current Architecture

The current monolithic CLI architecture can be gradually migrated:

1. **Phase 1**: Extract server components
2. **Phase 2**: Create client components
3. **Phase 3**: Add protocol servers
4. **Phase 4**: Enhance configuration management
5. **Phase 5**: Add production features

This migration preserves backward compatibility while enabling the new architecture. 