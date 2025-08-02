# Icebox Examples

This directory contains example code and tests for the Icebox project.

## icebox_native_test.go

This file contains comprehensive tests for the Icebox native protocol implementation, which provides ClickHouse-compatible connectivity.

### What it tests

1. **ClickHouse Go Client Connection** - Tests connection and ping functionality using the official ClickHouse Go driver
2. **ClickHouse Go Query Execution** - Tests basic query execution (currently expected to fail due to response format issues)
3. **Native Protocol Handshake** - Tests the low-level native protocol handshake process
4. **Native Protocol Query** - Tests query execution using the native protocol directly
5. **Native Protocol Ping** - Tests ping/pong functionality using the native protocol

### How to run

#### Prerequisites

1. Build the Icebox server:
   ```bash
   make build-server
   ```

2. Start the Icebox server:
   ```bash
   make run-server
   ```

#### Running the tests

```bash
# Run all tests
go test examples/icebox_native_test.go -v

# Run a specific test
go test examples/icebox_native_test.go -v -run TestClickHouseGoConnection

# Run tests with server check disabled (will fail if server not running)
go test examples/icebox_native_test.go -v -run TestNativeProtocolHandshake
```

### Expected behavior

- **With server running**: Tests will execute and show results
- **Without server running**: Tests will be skipped with helpful instructions

### Current status

- ✅ **Handshake**: Working correctly
- ✅ **Ping/Pong**: Working correctly  
- ✅ **Protocol compatibility**: Working correctly
- 🔧 **Query response format**: Needs fixes in the server implementation

### Troubleshooting

If tests fail with connection errors:

1. Ensure the server is running on port 9000
2. Check that the native protocol is enabled in the server configuration
3. Verify no firewall is blocking port 9000

### Development notes

The tests use both high-level (ClickHouse Go driver) and low-level (raw TCP) approaches to verify protocol compatibility. This helps ensure that both the protocol implementation and the driver integration are working correctly. 