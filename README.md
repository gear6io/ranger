# 🚀 Modern Data Lakehouse Platform

> **Note**: "Ranger" is a placeholder name and may change at any time. This project is actively being developed and transferred to the gear6io organization.

> **⚠️ EXPERIMENTAL**: This project is currently in experimental/alpha stage. Features may be incomplete, APIs may change, and it's not recommended for production use.

[![Go Report Card](https://goreportcard.com/badge/github.com/gear6io/ranger)](https://goreportcard.com/report/github.com/gear6io/ranger)
[![CI](https://github.com/gear6io/ranger/actions/workflows/ci.yml/badge.svg)](https://github.com/gear6io/ranger/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/gear6io/ranger)](https://golang.org)
[![License](https://img.shields.io/badge/License-BSL%201.1-blue.svg)](LICENSE)

## 🎯 What is this?

A data lakehouse platform built with Go that aims to provide Apache Iceberg table format support with multiple storage backends and query capabilities. Currently in **experimental development** with a focus on streaming architecture and memory efficiency.

**⚠️ Experimental Status**: This platform is designed for research, development, and experimentation. It's not yet production-ready and should be used for learning and prototyping purposes only.

## ✨ Key Features

- **🧊 Apache Iceberg**: Support for the Iceberg table format
- **⚡ Go Implementation**: Built with Go for performance and efficiency
- **🔌 Multiple Protocols**: HTTP, JDBC, and native binary protocol support
- **📊 DuckDB Integration**: SQL query engine integration
- **🔄 Streaming Architecture**: Memory-efficient data processing with batch operations
- **🛡️ Development Tools**: Error handling, logging, and monitoring capabilities
- **📁 Multiple Storage**: Memory, filesystem, and S3 storage backends
- **🏷️ Multiple Catalogs**: JSON, SQLite, and REST catalog support

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client SDK    │    │  HTTP Gateway   │    │  Native Server  │
│   (Go/HTTP)    │◄──►│   (Port 2847)   │◄──►│   (Port 2849)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 │
                    ┌─────────────────┐
                    │  Query Engine   │
                    │   (DuckDB)      │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Storage Manager │
                    │ (Memory/FS/S3)  │
                    └─────────────────┘
                                 │
                    ┌─────────────────┐
                    │ Metadata Store  │
                    │   (SQLite)      │
                    └─────────────────┘
```

**Note**: This architecture diagram represents the planned/experimental design. Some components may be in various stages of implementation.

## 🚀 Quick Start

### ⚠️ Experimental Usage Warning

This platform is in experimental development. Expect:
- **Incomplete features** and functionality
- **API changes** between versions
- **Limited documentation** and examples
- **Potential bugs** and instability
- **No production guarantees**

### Prerequisites

- **Go 1.24+** for building from source
- **DuckDB v1.3.0+** (automatically bundled)
- **Apache Iceberg** support (built-in)
- **Experimental mindset** - be prepared for things to break or change

### 1. Install the Platform

```bash
# Clone the repository
git clone https://github.com/gear6io/ranger.git
cd ranger

# Build the platform
make build-server
make build-client

# Or build individually
go build -o bin/ranger-server cmd/ranger-server/main.go
go build -o bin/ranger-client cmd/ranger-client/main.go
```

### 2. Start the Server

```bash
# Start the server with default configuration
./bin/ranger-server --config ranger-server.yml

# Or start in development mode
make dev-server
```

### 3. Connect with Client

```bash
# Start interactive shell
./bin/ranger-client shell

# Execute a query
./bin/ranger-client query "SHOW TABLES"

# Connect to specific server
./bin/ranger-client --server localhost:2849 shell
```

## 🏛️ Core Components

### **Storage Engines**
- **Memory**: In-memory storage for development and testing
- **Filesystem**: Local disk storage with Parquet optimization and streaming
- **S3**: Cloud-native object storage with MinIO integration

### **Catalog Systems**
- **JSON**: Version-control friendly, human-readable metadata
- **SQLite**: ACID-compliant catalog
- **REST**: HTTP-based catalog for distributed deployments

### **Query Processing**
- **DuckDB**: Analytical database engine
- **SQL Parser**: SQL parsing and validation
- **Streaming**: Memory-efficient processing with batch operations

### **Protocol Support**
- **HTTP/1.1**: RESTful API for web applications
- **JDBC**: PostgreSQL wire protocol compatibility
- **Native**: Binary protocol for Go applications

## 🔧 Configuration

### Server Configuration (`ranger-server.yml`)

```yaml
server:
  host: "0.0.0.0"
  port: 8080
  log_level: "info"

storage:
  type: "filesystem"
  data_path: "data"
  catalog:
    type: "sqlite"
    path: "data/catalog.db"

query:
  engine: "duckdb"
  max_memory: "2GB"
  temp_dir: "temp"
```

### Client Configuration (`ranger-client.yml`)

```yaml
server:
  address: "localhost:2849"
  auth:
    username: "default"
    password: ""
    database: "default"

settings:
  max_execution_time: 300
  timezone: "UTC"
```

## 🧪 Development

### ⚠️ Development Environment

This project is designed for developers and researchers who want to:
- **Experiment** with data lakehouse architectures
- **Learn** about Apache Iceberg implementations
- **Prototype** new data processing workflows
- **Contribute** to open-source data infrastructure

### Building from Source

```bash
# Build all components
make build-all

# Build specific components
make build-server
make build-client

# Cross-platform builds
make build-all-platforms

# Development mode
make dev-server
```

### Running Tests

```bash
# Unit tests
go test ./...

# Integration tests
make test-integration

# Specific package tests
go test ./server/storage/...
```

### Docker Development

```bash
# Build Docker images
make docker-build-server
make docker-build-client

# Run with Docker Compose
make docker-up

# Development environment
docker-compose up -d
```

## 🔌 Integration

### Go SDK

```go
import "github.com/gear6io/ranger/pkg/sdk"

// Create client
client, err := sdk.NewClient(&sdk.Options{
    Addr: []string{"localhost:2849"},
    Auth: sdk.Auth{
        Username: "default",
        Database: "default",
    },
})

// Execute query
rows, err := client.Query("SELECT * FROM users LIMIT 10")
```

### HTTP API

```bash
# Health check
curl http://localhost:2847/health

# Execute query
curl -X POST http://localhost:2847/query \
  -H "Content-Type: application/json" \
  -d '{"query": "SELECT COUNT(*) FROM users"}'
```

### JDBC Connection

```bash
# Connection string
jdbc:postgresql://localhost:2848/default

# Standard JDBC tools work out of the box
psql -h localhost -p 2848 -U default -d default
```

## 🚧 Project Status

**Current Status**: **Experimental Alpha** - Research and development phase

**Development Phase**: Active experimental development with frequent changes

**Production Readiness**: **NOT RECOMMENDED** for production use

**Target Audience**: 
- 🧪 **Researchers** exploring data lakehouse architectures
- 👨‍💻 **Developers** learning Apache Iceberg implementations
- 🔬 **Data Engineers** prototyping new workflows
- 🎓 **Students** studying distributed data systems

**Current Capabilities**:
- ✅ Server and client binaries
- ✅ Multiple storage backends (Memory, Filesystem, S3)
- ✅ Multiple catalog systems (JSON, SQLite, REST)
- ✅ HTTP, JDBC, and Native protocols
- ✅ DuckDB query engine integration
- ✅ Streaming storage architecture
- ✅ Go SDK with connection pooling

**In Development**:
- 🔄 Interactive SQL shell
- 🔄 Data import functionality
- 🔄 Table creation and management
- 🔄 Change Data Capture (CDC)
- 🔄 Advanced query optimization

**Experimental Features**:
- 🧪 Streaming data processing
- 🧪 Memory-efficient storage
- 🧪 Protocol implementations
- 🧪 Catalog integrations

## 🤝 Contributing

We welcome contributions from researchers, developers, and data enthusiasts! This is an experimental project, so we're particularly interested in:

- **Research contributions** on data lakehouse architectures
- **Experimental implementations** of new features
- **Performance analysis** and optimization research
- **Documentation** and educational content

Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Quick Start for Contributors

```bash
# Fork and clone
git clone https://github.com/your-username/ranger.git
cd ranger

# Install development tools
make install-dev-tools

# Run tests
make test

# Submit a pull request
```

## 📚 Documentation

- [Contributing Guide](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)
- [Development Milestones](milestones/README.md)
- [Storage Architecture](server/storage/README.md)
- [SDK Documentation](pkg/sdk/README.md)

## 📄 License

This project is licensed under the **Business Source License 1.1 (BSL)** by MariaDB Corporation Ab.

**Current Phase (Open Phase)**: Until 2025-01-01, you can use this code under the Apache License, Version 2.0.

**After 2025-01-01**: The code will be licensed under the Business Source License, which means:
- You can still use the code for internal purposes
- You can still modify and distribute the code
- You cannot use it to provide a Database Service to third parties
- You cannot use it in a commercial product that competes with gear6io's commercial offerings

For the complete Business Source License text, see: https://mariadb.com/bsl11/

For questions about licensing, please contact: hello@gear6io.com

## 🙏 Acknowledgments

- **Apache Iceberg** for the table format specification
- **DuckDB** for the high-performance query engine
- **Go Community** for the excellent ecosystem and tooling
- **Research Community** for exploring data lakehouse architectures

## 📞 Support

- **Issues**: [GitHub Issues](https://github.com/gear6io/ranger/issues)
- **Discussions**: [GitHub Discussions](https://github.com/gear6io/ranger/discussions)
- **Security**: [Security Policy](SECURITY.md)

**⚠️ Support Level**: Limited support available - this is an experimental project

---

**Note**: This project is actively being developed and transferred. The "Ranger" name is temporary and subject to change. This is an **experimental platform** designed for research and development, not production use. For the latest updates, please check the repository regularly.
