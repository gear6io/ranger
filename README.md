# 🧊 Icebox

<div align="center">

**A single-binary playground for Apache Iceberg**  
*Five minutes to first query*

[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?style=flat&logo=go&logoColor=white)](https://golang.org)
[![Apache Iceberg](https://img.shields.io/badge/Apache%20Iceberg-v0.3.0--rc0-326ce5?style=flat&logo=apache&logoColor=white)](https://iceberg.apache.org)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![CI](https://github.com/TFMV/icebox/actions/workflows/ci.yml/badge.svg)](https://github.com/TFMV/icebox/actions/workflows/ci.yml)

[Quick Start](#-quick-start) • [Features](#-features) • [Usage Guide](docs/usage.md) • [Contributing](#-contributing)

</div>

---

## 🎯 What is Icebox?

Icebox is a **zero-configuration data lakehouse** that gets you from zero to querying Iceberg tables in under five minutes. Perfect for:

- 🔬 **Experimenting** with Apache Iceberg table format
- 📚 **Learning** lakehouse concepts and workflows  
- 🧪 **Prototyping** data pipelines locally
- 🚀 **Testing** Iceberg integrations before production

**No servers, no complex setup, no dependencies** - just a single binary and your data.

## 📈 Project Status

Icebox is alpha software—functional, fast-moving, and rapidly evolving.

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

### 1. Install Icebox

```bash
# Build from source
git clone https://github.com/TFMV/icebox.git
cd icebox
go build -o icebox cmd/icebox/main.go

# Add to your PATH for global access
sudo mv icebox /usr/local/bin/
# Or add the current directory to PATH
export PATH=$PATH:$(pwd)
```

**💡 Tip:** Add `export PATH=$PATH:/usr/local/bin` to your shell profile (`.bashrc`, `.zshrc`) for permanent access.

### 2. Initialize a Project

```bash
# Create a new lakehouse project (default: SQLite catalog)
./icebox init my-lakehouse
cd my-lakehouse

# Or with JSON catalog for version control friendly development
./icebox init my-lakehouse --catalog json
cd my-lakehouse
```

### 3. Import Your Data

```bash
# Import a Parquet or Avro file into an Iceberg table
./icebox import data.parquet --table sales
# or
./icebox import data.avro --table users

✅ Successfully imported table!

📊 Import Results:
   Table: [default sales]
   Records: 1,000,000
   Size: 45.2 MB
   Location: file:///.icebox/data/default/sales
```

### 3.5. Create Optimized Tables (Optional)

```bash
# Create tables with partitioning and sorting for better performance
./icebox table create analytics_events \
  --partition-by "date,region" \
  --sort-by "timestamp ASC,user_id ASC" \
  --schema events_schema.json

✅ Successfully created table!
✅ Applied partition specification with 2 field(s)
✅ Applied sort order with 2 field(s)

# Import data into the optimized table
./icebox import events.parquet --table analytics_events
```

### 4. Query Your Data

```bash
# Run SQL queries
./icebox sql "SELECT COUNT(*) FROM sales"
📋 Registered 1 tables for querying
⏱️  Query executed in 45ms
📊 1 rows returned
┌─────────────┐
│ count_star()│
├─────────────┤
│ 1000000     │
└─────────────┘

# Use the interactive shell for complex analysis
./icebox shell

🧊 Icebox SQL Shell v0.1.0
Interactive SQL querying for Apache Iceberg
Type \help for help, \quit to exit

icebox> SELECT region, AVG(amount) as avg_amount FROM sales GROUP BY region;
⏱️  Query executed in 23ms
📊 3 rows returned
┌─────────────┬────────────┐
│ region      │ avg_amount │
├─────────────┼────────────┤
│ North       │ 1250.50    │
│ South       │ 980.75     │
│ West        │ 1450.25    │
└─────────────┴────────────┘

icebox> \quit
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

Icebox is designed to be **approachable for developers** at all levels.

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
git clone https://github.com/TFMV/icebox.git
cd icebox
go mod tidy
go build -o icebox cmd/icebox/main.go

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

[⭐ Star this project](https://github.com/TFMV/icebox) • [📚 Usage Guide](docs/usage.md) • [🐛 Report Issue](https://github.com/TFMV/icebox/issues)

</div>
