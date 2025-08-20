# Icebox Go SDK

A high-performance, feature-rich Go SDK for Icebox, inspired by the excellent design patterns from `clickhouse-go` and `ch-go`. This SDK provides a native implementation with connection pooling, compression, comprehensive error handling, and full database/sql compatibility.

## Features

### 🚀 **High Performance**
- **Connection Pooling**: Efficient connection management with configurable pool sizes
- **Compression Support**: Multiple compression algorithms (LZ4, ZSTD, GZIP, Brotli)
- **Batch Operations**: High-performance batch insertions with column-level operations
- **Async Operations**: Non-blocking asynchronous inserts

### 🔧 **Developer Friendly**
- **DSN Support**: Simple connection strings (`icebox://user:pass@host:port/db`)
- **Database/sql Compatible**: Full compatibility with Go's standard database/sql package
- **Comprehensive Settings**: Rich configuration options for query optimization
- **Context Support**: Full context.Context integration for timeouts and cancellation

### 🛡️ **Production Ready**
- **Error Handling**: Detailed error types with server exception support
- **Logging**: Structured logging with zap integration
- **Connection Strategies**: Multiple connection distribution strategies
- **Health Checks**: Built-in ping and connection validation

### 📊 **Advanced Features**
- **Multiple Protocols**: Support for both Native and HTTP protocols
- **TLS Support**: Secure connections with custom TLS configuration
- **Query Options**: Flexible query execution with custom settings
- **Statistics**: Connection pool monitoring and statistics

## Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/TFMV/icebox/pkg/sdk"
    "go.uber.org/zap"
)

func main() {
    // Create logger
    logger, _ := zap.NewDevelopment()
    defer logger.Sync()

    // Create client options
    opt := &sdk.Options{
        Logger: logger,
        Addr:   []string{"127.0.0.1:9000"},
        Auth: sdk.Auth{
            Username: "default",
            Password: "",
            Database: "default",
        },
        Settings: sdk.Settings{
            "max_execution_time": 30,
            "timezone":          "UTC",
        },
    }

    // Create client
    client, err := sdk.NewClient(opt)
    if err != nil {
        log.Fatalf("Failed to create client: %v", err)
    }
    defer client.Close()

    // Test connection
    if err := client.Ping(context.Background()); err != nil {
        log.Fatalf("Failed to ping: %v", err)
    }

    fmt.Println("Connected to Icebox server!")
}
```

### Using DSN (Connection String)

```go
// Parse DSN
dsn := "icebox://user:password@localhost:9000/mydb?max_execution_time=60&timezone=UTC"
opt, err := sdk.ParseDSN(dsn)
if err != nil {
    log.Fatalf("Failed to parse DSN: %v", err)
}

// Create client from DSN
client, err := sdk.Open(opt)
if err != nil {
    log.Fatalf("Failed to open connection: %v", err)
}
defer client.Close()
```

### Database/sql Compatibility

```go
// Open database using DSN
db, err := sdk.OpenDB(&sdk.Options{
    Addr: []string{"127.0.0.1:9000"},
    Auth: sdk.Auth{
        Username: "default",
        Database: "default",
    },
})
if err != nil {
    log.Fatalf("Failed to open database: %v", err)
}
defer db.Close()

// Use standard database/sql interface
rows, err := db.Query("SELECT id, name FROM test_table LIMIT 5")
if err != nil {
    log.Fatalf("Failed to query: %v", err)
}
defer rows.Close()

for rows.Next() {
    var id int
    var name string
    if err := rows.Scan(&id, &name); err != nil {
        log.Fatalf("Failed to scan: %v", err)
    }
    fmt.Printf("Row: id=%d, name=%s\n", id, name)
}
```

## Advanced Features

### Connection Pooling

```go
opt := &sdk.Options{
    Addr:            []string{"127.0.0.1:9000"},
    MaxOpenConns:    20,                    // Maximum open connections
    MaxIdleConns:    10,                    // Maximum idle connections
    ConnMaxLifetime: 30 * time.Minute,      // Connection lifetime
    ConnOpenStrategy: sdk.ConnOpenRoundRobin, // Connection distribution strategy
}

client, err := sdk.NewClient(opt)
if err != nil {
    log.Fatalf("Failed to create client: %v", err)
}

// Get connection statistics
stats := client.Stats()
fmt.Printf("Connection pool stats: %+v\n", stats)
```

### Compression

```go
opt := &sdk.Options{
    Addr: []string{"127.0.0.1:9000"},
    Compression: &sdk.Compression{
        Method: sdk.CompressionLZ4,  // LZ4, ZSTD, GZIP, Brotli, etc.
        Level:  1,                   // Compression level
    },
}

client, err := sdk.NewClient(opt)
```

### Batch Operations

```go
ctx := context.Background()

// Prepare batch
batch, err := client.PrepareBatch(ctx, "INSERT INTO test_table (id, name, value, created_at)")
if err != nil {
    log.Fatalf("Failed to prepare batch: %v", err)
}

// Add rows to batch
now := time.Now()
for i := 1; i <= 1000; i++ {
    err := batch.Append(
        i,                                     // id
        fmt.Sprintf("name_%d", i),             // name
        float64(i)*1.23,                       // value
        now.Add(time.Duration(i)*time.Second), // created_at
    )
    if err != nil {
        log.Fatalf("Failed to append row: %v", err)
    }
}

// Send batch
if err := batch.Send(); err != nil {
    log.Fatalf("Failed to send batch: %v", err)
}

fmt.Printf("Successfully inserted %d rows\n", batch.Rows())
```

### Column-Level Batch Operations

```go
// Prepare batch
batch, err := client.PrepareBatch(ctx, "INSERT INTO test_table (id, name, value, created_at)")
if err != nil {
    log.Fatalf("Failed to prepare batch: %v", err)
}

// Get column references for direct manipulation
idCol := batch.Column(0)    // id column
nameCol := batch.Column(1)  // name column
valueCol := batch.Column(2) // value column
timeCol := batch.Column(3)  // created_at column

// Add data using column-specific operations
now := time.Now()
for i := 1; i <= 100; i++ {
    idCol.Append(i)
    nameCol.Append(fmt.Sprintf("name_%d", i))
    valueCol.Append(float64(i) * 1.23)
    timeCol.Append(now.Add(time.Duration(i) * time.Second))
}

// Send batch
if err := batch.Send(); err != nil {
    log.Fatalf("Failed to send batch: %v", err)
}
```

### Async Operations

```go
// Async insert without waiting
err = client.AsyncInsert(ctx, "INSERT INTO test_table (id, name) VALUES (?, ?)", false, 1, "async_test")
if err != nil {
    log.Fatalf("Failed to async insert: %v", err)
}
```

### Multiple Server Addresses

```go
opt := &sdk.Options{
    Addr: []string{
        "server1:9000",
        "server2:9000",
        "server3:9000",
    },
    ConnOpenStrategy: sdk.ConnOpenRoundRobin,
    MaxOpenConns:     10,
    MaxIdleConns:     5,
}

client, err := sdk.NewClient(opt)
```

### Advanced Settings

```go
opt := &sdk.Options{
    Addr: []string{"127.0.0.1:9000"},
    Settings: sdk.Settings{
        "max_execution_time":                       60,
        "timezone":                                 "UTC",
        "date_time_input_format":                   "best_effort",
        "max_block_size":                           10000,
        "enable_optimize_predicate_expression":     true,
        "max_memory_usage":                         1000000000, // 1GB
        "max_bytes_before_external_group_by":      1000000000, // 1GB
    },
}
```

### Error Handling

```go
rows, err := client.Query(ctx, "SELECT * FROM non_existent_table")
if err != nil {
    // Check if it's an exception from the server
    if exception, ok := err.(*sdk.Exception); ok {
        fmt.Printf("Server exception: %s (code: %d)\n", exception.Message, exception.Code)
    } else {
        fmt.Printf("Connection error: %v\n", err)
    }
    return
}
defer rows.Close()
```

### Context and Timeouts

```go
// Create context with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

// Execute query with context
rows, err := client.Query(ctx, "SELECT * FROM test_table")
if err != nil {
    if err == context.DeadlineExceeded {
        fmt.Println("Query timed out")
    } else {
        fmt.Printf("Query failed: %v\n", err)
    }
    return
}
defer rows.Close()
```

## Configuration Options

### Options Structure

```go
type Options struct {
    // Protocol and connection
    Protocol   Protocol
    Addr       []string
    Auth       Auth
    DialContext func(ctx context.Context, addr string) (net.Conn, error)
    DialStrategy func(ctx context.Context, connID int, options *Options, dial Dial) (DialResult, error)
    
    // TLS and security
    TLS *tls.Config
    
    // Compression
    Compression *Compression
    
    // Connection pooling
    MaxOpenConns     int           // default 10
    MaxIdleConns     int           // default 5
    ConnMaxLifetime  time.Duration // default 1 hour
    ConnOpenStrategy ConnOpenStrategy
    
    // Timeouts
    DialTimeout  time.Duration // default 30 seconds
    ReadTimeout  time.Duration // default 3 seconds
    WriteTimeout time.Duration // default 3 seconds
    
    // Settings and configuration
    Settings Settings
    Debug    bool
    Debugf   func(format string, v ...any)
    
    // HTTP specific (for HTTP protocol)
    HTTPHeaders         map[string]string
    HTTPURLPath         string
    HTTPMaxConnsPerHost int
    HTTPProxyURL        *url.URL
    
    // Performance
    BlockBufferSize      uint8 // default 2
    MaxCompressionBuffer int   // default 10MB
    FreeBufOnConnRelease bool  // default false
    
    // Logging
    Logger *zap.Logger
}
```

### Compression Methods

```go
const (
    CompressionNone    = CompressionMethod(0)
    CompressionLZ4     = CompressionMethod(1)
    CompressionLZ4HC   = CompressionMethod(2)
    CompressionZSTD    = CompressionMethod(3)
    CompressionGZIP    = CompressionMethod(4)
    CompressionDeflate = CompressionMethod(5)
    CompressionBrotli  = CompressionMethod(6)
)
```

### Connection Strategies

```go
const (
    ConnOpenInOrder ConnOpenStrategy = iota
    ConnOpenRoundRobin
    ConnOpenRandom
)
```

### Protocols

```go
const (
    Native Protocol = iota
    HTTP
)
```

## Performance Considerations

### Connection Pooling
- Set appropriate `MaxOpenConns` based on your workload
- Use `MaxIdleConns` to keep warm connections ready
- Monitor connection statistics with `client.Stats()`

### Batch Operations
- Use batch operations for bulk inserts
- Consider column-level operations for better performance
- Use appropriate batch sizes (1000-10000 rows per batch)

### Compression
- LZ4 provides good compression with low CPU overhead
- ZSTD offers better compression ratios
- GZIP is widely supported but slower

### Settings Optimization
- Adjust `max_block_size` based on your data characteristics
- Use `max_memory_usage` to control memory consumption
- Enable query optimizations with `enable_optimize_predicate_expression`

## Error Handling

The SDK provides comprehensive error handling with specific error types:

```go
var (
    ErrBatchInvalid              = errors.New("icebox: batch is invalid. check appended data is correct")
    ErrBatchAlreadySent          = errors.New("icebox: batch has already been sent")
    ErrBatchNotSent              = errors.New("icebox: invalid retry, batch not sent yet")
    ErrAcquireConnTimeout        = errors.New("icebox: acquire conn timeout. you can increase the number of max open conn or the dial timeout")
    ErrUnsupportedServerRevision = errors.New("icebox: unsupported server revision")
    ErrBindMixedParamsFormats    = errors.New("icebox [bind]: mixed named, numeric or positional parameters")
    ErrAcquireConnNoAddress      = errors.New("icebox: no valid address supplied")
    ErrServerUnexpectedData      = errors.New("code: 101, message: Unexpected packet Data received from client")
)
```

### Server Exceptions

```go
type Exception struct {
    Code    int
    Name    string
    Message string
    Stack   string
}
```

## Examples

See the `example.go` file for comprehensive examples covering all SDK features:

- Basic usage and connection
- DSN parsing and connection strings
- Connection pooling and statistics
- Compression configuration
- Query execution and result scanning
- Batch operations with various options
- Column-level batch operations
- Async operations
- Settings and configuration
- Database/sql compatibility
- Error handling patterns
- Context and timeout usage
- Multiple server addresses

## Installation

```bash
go get github.com/TFMV/icebox/pkg/sdk
```

## Dependencies

- `github.com/go-faster/errors` - Enhanced error handling
- `github.com/google/uuid` - UUID generation for query IDs
- `go.uber.org/zap` - Structured logging
- Standard library packages: `context`, `database/sql`, `net`, `time`, etc.

## License

This SDK is part of the Icebox project and follows the same license terms.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Performance Benchmarks

The SDK is designed for high performance with features like:
- Efficient connection pooling
- Zero-copy data handling where possible
- Optimized batch operations
- Compression support for reduced network overhead
- Minimal memory allocations

For specific performance characteristics, run the included benchmarks in the test suite.
