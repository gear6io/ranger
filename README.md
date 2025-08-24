# 🧊 Data Lakehouse

A modern, high-performance data lakehouse platform built with Go.

[![Go Report Card](https://goreportcard.com/badge/github.com/gear6io/ranger)](https://goreportcard.com/report/github.com/gear6io/ranger)
[![CI](https://github.com/gear6io/ranger/actions/workflows/ci.yml/badge.svg)](https://github.com/gear6io/ranger/actions/workflows/ci.yml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/gear6io/ranger)](https://golang.org)
[![License](https://img.shields.io/github/license/gear6io/ranger)](LICENSE)

## 🎯 What is this?

This is a **zero-configuration data lakehouse** that gets you from zero to querying Iceberg tables in under five minutes. Perfect for:

- 🔬 **Experimenting** with Apache Iceberg table format
- 📚 **Learning** lakehouse concepts and workflows  
- 🧪 **Prototyping** data pipelines locally
- 🚀 **Testing** Iceberg integrations before production

**No servers, no complex setup, no dependencies** - just a single binary and your data.

## 📈 Project Status

This is alpha software—functional, fast-moving, and rapidly evolving.

The core is there.
Now we're looking for early contributors to help shape what comes next—whether through code, docs, testing, or ideas.

## ✨ Features

- **Single binary** - No installation complexity
- **Embedded catalog** - SQLite-based, no external database needed
- **JSON catalog** - Local JSON-based catalog for development and prototyping
- **REST catalog support** - Connect to existing Iceberg REST catalogs  
- **Embedded MinIO server** - S3-compatible storage for testing production workflows
- **Parquet & Avro import** with automatic schema inference
- **Enhanced table creation** - Full support for partitioning and sort orders
- **DuckDB v1.3.0 integration** - High-performance analytics with native Iceberg support
- **Universal catalog compatibility** - All catalog types work seamlessly with query engine
- **Interactive SQL shell** with command history and multi-line support
- **Time-travel queries** - Query tables at any point in their history
- **Transaction support** with proper ACID guarantees

## 🚀 Quick Start

### Prerequisites

- **Go 1.21+** for building from source
- **DuckDB v1.3.0+** for optimal Iceberg support (automatically bundled with Go driver)

### 1. Install the Platform

```bash
# Clone the repository
git clone https://github.com/gear6io/ranger.git
cd ranger
go build -o ranger cmd/ranger-server/main.go

# Optional: Install globally
sudo mv ranger /usr/local/bin/
```

### 2. Initialize Your Data Lakehouse

```bash
# Initialize a new data lakehouse
./ranger init my-lakehouse

# Or with specific catalog type
./ranger init my-lakehouse --catalog json
```

### 3. Import Your Data

```bash
# Import Parquet files
./ranger import data.parquet --table sales

# Import Avro files
./ranger import data.avro --table users
```

### 4. Query Your Data

```bash
# Query using SQL
./ranger sql "SELECT * FROM sales WHERE amount > 1000"

# Location: file:///.metadata/data/default/sales
```

### 5. Create Tables

```bash
# Create a new table
./ranger table create analytics_events \
  --schema "id:int,event:string,timestamp:timestamp,user_id:string" \
  --partition-by "date(timestamp),user_id"
```

**🎉 You now have a working Iceberg lakehouse with your data and SQL querying!**

## 🌐 Storage & Catalog Support

| Storage Type | Description | Use Case |
|-------------|-------------|----------|
| **Local Filesystem** | File-based storage | Development, testing |
| **In-Memory** | Temporary fast storage | Unit testing, experiments |
| **Embedded MinIO** | S3-compatible local server | Cloud workflow testing |
| **External MinIO** | Remote MinIO instance | Shared development |

| Catalog Type | Description | Use Case |
|-------------|-------------|----------|
| **SQLite** | Embedded local catalog | Single-user development |
| **JSON** | Local JSON-based catalog | Development, prototyping, embedded use |
| **REST** | External Iceberg REST catalog | Multi-user, production |

## 🤝 Contributing

The platform is designed to be **approachable for developers** at all levels.

### Quick Contribution Guide

1. **🍴 Fork** the repository and create a feature branch
2. **🧪 Write tests** for your changes
3. **📝 Update documentation** as needed
4. **✅ Ensure tests pass** with `go test ./...`
5. **🔄 Submit a pull request**

### Development

```bash
# Prerequisites: Go 1.21+, DuckDB v1.3.0+ (for local CLI testing)
# Install DuckDB locally (optional, for CLI testing)
# macOS: brew install duckdb
# Linux: See https://duckdb.org/docs/installation/

# Build from source
git clone https://github.com/gear6io/ranger.git
cd ranger
go mod tidy
go build -o ranger cmd/ranger-server/main.go

# Run tests
go test ./...

# Add to PATH for development
export PATH=$PATH:$(pwd)
```

### Areas for Contribution

- 🐛 **Bug fixes** and stability improvements
- 📚 **Documentation** and examples  
- ✨ **New features** and enhancements
- 🧪 **Test coverage** improvements
- 🎨 **CLI/UX** enhancements

## 📚 Documentation

For comprehensive documentation and advanced features, see our **[📚 Usage Guide](docs/usage.md)**.

## 📄 License

This project is licensed under the **Apache License 2.0** - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**Made with ❤️ for the data community**

[⭐ Star this project](https://github.com/gear6io/ranger) • [📚 Usage Guide](docs/usage.md) • [🐛 Report Issue](https://github.com/gear6io/ranger/issues)

</div>
# Test comment
