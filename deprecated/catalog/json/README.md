# 🧊 Icebox JSON Catalog for Apache Iceberg

A **feature-complete**, local-first JSON-based catalog for Apache Iceberg for sandbox environments, embedded use cases, and rapid prototyping.

> ⚡ Built into [Icebox](https://github.com/TFMV/icebox)  
> 🪶 Inspired by [`boring-catalog`](https://github.com/boringdata/boring-catalog). Extended for full fidelity and concurrent workloads.

---

## 🚀 Highlights

- **Thread-safe & atomic**: Full concurrency protection using RW mutexes and atomic file writes
- **No dependencies**: Pure Go, no external services, and zero setup required
- **Iceberg compliant**: Fully aligns with the Iceberg v2 metadata specification
- **Optimistic concurrency**: Uses ETags to prevent race conditions
- **Metrics & observability**: Tracks catalog ops, cache behavior, and detailed errors
- **Type-complete**: Handles all Iceberg types — primitive, decimal, list, map, struct, and more
- **Graceful fallbacks**: Load from `.icebox/index` for embedded catalog autodiscovery

---

## ✨ Features

### 🏗️ Core Architecture

- Atomic JSON file writes with temporary files and `os.Rename`
- Safe for concurrent access via read/write mutexes
- Schema validation and integrity checks on load
- Clean separation of catalog, namespace, and table structures

### 📊 Observability

- Built-in metrics: table/namespace creation, cache hits/misses, operation errors
- Structured logging with context-aware messages
- Optional TTL-based cache layer with read-through logic

### 💪 Iceberg Support

- Fully supports the Iceberg v2 table spec
- Field ID tracking for schema evolution
- Accurate partition specs, sort orders, and snapshot log scaffolding
- UUID v4 table generation via `google/uuid`

### 🔁 Developer UX

- Supports registration of existing tables with known metadata
- Resilient startup via `.icebox/index` fallback
- Compatible with the `iceberg-go` interface for transparent drop-in usage
