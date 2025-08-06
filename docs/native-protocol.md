# Icebox Native Protocol

This document describes the native protocol implementation in Icebox, which provides ClickHouse-compatible connectivity for high-performance database access.

## Overview

The native protocol allows clients to connect to Icebox using the ClickHouse native protocol, enabling:
- High-performance binary communication
- Support for the `clickhouse-go` driver
- Compatibility with ClickHouse client tools
- Efficient data transfer with minimal overhead

## Architecture

### Protocol Server
The native protocol server is implemented in `server/protocols/native/` and consists of:

- **`server.go`**: Main server implementation that accepts connections
- **`connection.go`**: Handles individual client connections
- **`protocol.go`**: Defines packet types and constants
- **`packet.go`**: Binary packet reading/writing utilities

### Packet Types

#### Client Packets
- `ClientHello` (0): Initial handshake packet
- `ClientQuery` (1): SQL query execution
- `ClientData` (2): Data insertion
- `ClientPing` (3): Keep-alive ping
- `ClientCancel` (4): Query cancellation

#### Server Packets
- `ServerHello` (0): Server handshake response
- `ServerData` (1): Query results
- `ServerException` (2): Error responses
- `ServerProgress` (3): Query progress updates
- `ServerPong` (4): Ping response
- `ServerEndOfStream` (5): End of data stream

## Configuration

The native protocol is configured in `icebox-server.yml`:

```yaml
native:
  enabled: true
  address: "0.0.0.0"
  port: 9000
```

## Usage Examples

### Using clickhouse-go Driver

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func main() {
    // Connect to Icebox native server
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
        Settings: clickhouse.Settings{
            "max_execution_time": 60,
        },
        Debug: true,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Test the connection
    if err := conn.Ping(context.Background()); err != nil {
        log.Fatal(err)
    }

    fmt.Println("Successfully connected to Icebox native server!")

    // Execute a simple query
    if err := conn.Exec(context.Background(), "SELECT 1"); err != nil {
        log.Fatal(err)
    }

    fmt.Println("Query executed successfully")
}
```

### Using database/sql Interface

```go
package main

import (
    "context"
    "database/sql"
    "fmt"
    "log"

    "github.com/ClickHouse/clickhouse-go/v2"
)

func main() {
    // Open connection using database/sql
    db := clickhouse.OpenDB(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
    })
    defer db.Close()

    // Test connection
    ctx := context.Background()
    if err := db.PingContext(ctx); err != nil {
        log.Fatal(err)
    }

    fmt.Println("Database/sql connection successful!")

    // Execute query
    rows, err := db.QueryContext(ctx, "SELECT 'Hello from Icebox!' as message")
    if err != nil {
        log.Fatal(err)
    }
    defer rows.Close()

    for rows.Next() {
        var message string
        if err := rows.Scan(&message); err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Result: %s\n", message)
    }
}
```

## Protocol Flow

### Connection Establishment

1. **Client Hello**: Client sends `ClientHello` packet with:
   - Client name and version
   - Database name
   - Username and password
   - Client capabilities

2. **Server Hello**: Server responds with `ServerHello` packet containing:
   - Server name and version
   - Default database
   - Server capabilities

3. **Authentication**: Server validates credentials (currently placeholder)

### Query Execution

1. **Query Packet**: Client sends `ClientQuery` with:
   - Query ID
   - Client info
   - SQL query string

2. **Query Processing**: Server processes the query using the engine

3. **Results**: Server sends:
   - Column metadata
   - Data blocks
   - Progress updates
   - End of stream marker

### Data Insertion

1. **Data Packet**: Client sends `ClientData` with:
   - Table name
   - Client info
   - External table info
   - Block info
   - Column metadata (name and type)
   - Row count
   - Data rows

2. **Processing**: Server processes the data insertion:
   - Validates table and column metadata
   - Reads and processes data rows
   - Writes data to storage system

3. **Response**: Server sends:
   - Progress updates
   - Completion status
   - End of stream marker

### Batch Insert Support

The native protocol now supports batch insert operations from the ClickHouse-go SDK. This enables efficient bulk data insertion with the following features:

- **Column Metadata**: Automatic parsing of column names and types
- **Multiple Data Types**: Support for UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64, String, DateTime, Date
- **Batch Processing**: Efficient handling of multiple rows in a single operation
- **Progress Tracking**: Real-time progress updates during batch operations

#### Batch Insert Example

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/ClickHouse/clickhouse-go/v2"
    "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

func main() {
    // Connect to Icebox native server
    conn, err := clickhouse.Open(&clickhouse.Options{
        Addr: []string{"localhost:9000"},
        Auth: clickhouse.Auth{
            Database: "default",
            Username: "default",
            Password: "",
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer conn.Close()

    // Create test table
    if err := conn.Exec(context.Background(), `
        CREATE TABLE IF NOT EXISTS users (
            id UInt32,
            name String,
            email String,
            created_at DateTime
        ) ENGINE = Memory
    `); err != nil {
        log.Fatal(err)
    }

    // Prepare batch insert
    batch, err := conn.PrepareBatch(context.Background(), 
        "INSERT INTO users (id, name, email, created_at)")
    if err != nil {
        log.Fatal(err)
    }

    // Add rows to batch
    now := time.Now()
    for i := 1; i <= 1000; i++ {
        if err := batch.Append(
            uint32(i),
            fmt.Sprintf("User %d", i),
            fmt.Sprintf("user%d@example.com", i),
            now.Add(time.Duration(i)*time.Second),
        ); err != nil {
            log.Fatal(err)
        }
    }

    // Execute batch
    if err := batch.Send(); err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Successfully inserted %d rows\n", batch.Rows())
}
```

#### Supported Data Types

The batch insert functionality supports the following ClickHouse data types:

| ClickHouse Type | Go Type | Description |
|----------------|---------|-------------|
| UInt8 | uint8 | 8-bit unsigned integer |
| UInt16 | uint16 | 16-bit unsigned integer |
| UInt32 | uint32 | 32-bit unsigned integer |
| UInt64 | uint64 | 64-bit unsigned integer |
| Int8 | int8 | 8-bit signed integer |
| Int16 | int16 | 16-bit signed integer |
| Int32 | int32 | 32-bit signed integer |
| Int64 | int64 | 64-bit signed integer |
| Float32 | float32 | 32-bit floating point |
| Float64 | float64 | 64-bit floating point |
| String | string | Variable-length string |
| DateTime | time.Time | Date and time |
| Date | time.Time | Date only |

## Implementation Status

### ✅ Implemented
- Basic protocol server structure
- Connection handling
- Packet reading/writing
- Client hello handshake
- Query packet handling (placeholder responses)
- Ping/pong support
- **Batch insert support with ClickHouse-go SDK**
- **Column metadata parsing**
- **Multiple data type support**
- **Progress tracking for batch operations**

### 🔄 In Progress
- Query execution integration with engine
- Data insertion storage integration
- Error handling and exceptions
- Authentication system

### 📋 Planned
- Compression support
- Prepared statements
- Connection pooling
- SSL/TLS support
- Advanced data type support (Arrays, Maps, etc.)

## Testing

### Manual Testing
1. Start the server:
   ```bash
   ./icebox-server
   ```

2. Run the example client:
   ```bash
   go run examples/clickhouse_go_example.go
   ```

3. Test with clickhouse-client:
   ```bash
   clickhouse-client --host localhost --port 9000
   ```

### Integration Testing
The native protocol includes integration tests in `integration_tests/native_test.go` that verify:
- Connection establishment
- Query execution
- Data transfer
- Error handling

## Performance Considerations

- **Binary Protocol**: Uses efficient binary encoding for minimal overhead
- **Connection Pooling**: Supports multiple concurrent connections
- **Buffered I/O**: Implements buffered reading/writing for better performance
- **Async Processing**: Handles connections asynchronously

## Security

### Current Implementation
- Basic authentication structure (placeholder)
- No encryption (planned for future)

### Planned Security Features
- TLS/SSL encryption
- User authentication
- Role-based access control
- Connection rate limiting

## Troubleshooting

### Common Issues

1. **Connection Refused**
   - Ensure server is running on correct port
   - Check firewall settings
   - Verify configuration

2. **Protocol Errors**
   - Check client version compatibility
   - Verify packet format
   - Review server logs

3. **Authentication Failures**
   - Verify username/password
   - Check database permissions
   - Review authentication configuration

### Debugging

Enable debug logging in the server configuration:

```yaml
log:
  level: "debug"
```

This will provide detailed protocol packet information for troubleshooting.

## Future Enhancements

1. **Full Query Engine Integration**: Connect to actual Iceberg query engine
2. **Advanced Features**: Support for all ClickHouse protocol features
3. **Performance Optimization**: Implement connection pooling and caching
4. **Security**: Add TLS encryption and proper authentication
5. **Monitoring**: Add metrics and health checks
6. **Load Balancing**: Support for multiple server instances

## References

- [ClickHouse Native Protocol](https://clickhouse.com/docs/en/interfaces/tcp)
- [clickhouse-go Driver](https://github.com/ClickHouse/clickhouse-go)
- [Apache Iceberg](https://iceberg.apache.org/) 