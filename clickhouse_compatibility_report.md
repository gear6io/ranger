# ClickHouse Client Compatibility Test Report

## Test Summary

**Date:** August 2, 2025  
**Icebox Server Version:** Running on localhost:9000  
**ClickHouse Client Version:** 25.7.1.3997 (official build)  
**Test File:** `/tmp/icebox_clickhouse_compatibility_test_20250802_225836.log`

## Test Results

### Connection Status
✅ **Connection Successful**: The clickhouse-client successfully connects to the icebox server  
❌ **Protocol Compatibility**: All queries fail with the same error

### Error Analysis
**Error Code:** 261 (UNKNOWN_BLOCK_INFO_FIELD)  
**Error Message:** `Unknown BlockInfo field number: 114: while receiving packet from localhost:9000`

**Root Cause:** The icebox server is sending a field number 114 (ASCII character 'r') that the clickhouse-client doesn't recognize as a valid BlockInfo field.

### Tested Queries
All 17 test queries failed with the same error:

1. `SELECT 1` - ❌ Failed
2. `SELECT 42` - ❌ Failed  
3. `SELECT 'hello'` - ❌ Failed
4. `SELECT now()` - ❌ Failed
5. `SELECT count(*)` - ❌ Failed
6. `SELECT * FROM users` - ❌ Failed
7. `SELECT * FROM orders` - ❌ Failed
8. `SELECT * FROM unknown_table` - ❌ Failed
9. `SELECT 1 as test_column` - ❌ Failed
10. `SELECT 1, 2, 3` - ❌ Failed
11. `SELECT 'test' as string_column, 123 as int_column` - ❌ Failed
12. `SELECT current_database()` - ❌ Failed
13. `SELECT version()` - ❌ Failed
14. `SHOW DATABASES` - ❌ Failed
15. `SHOW TABLES` - ❌ Failed
16. `DESCRIBE users` - ❌ Failed
17. `DESCRIBE orders` - ❌ Failed

## Technical Analysis

### Protocol Issue
The error occurs because our icebox server's native protocol implementation is not fully compatible with the ClickHouse client protocol. Specifically:

1. **BlockInfo Field Number 114**: The character 'r' (ASCII 114) is being sent as a BlockInfo field number
2. **Protocol Format**: Our server is likely missing required protocol fields or sending them in incorrect format
3. **Data Packet Structure**: The ClickHouse client expects a specific packet structure that our implementation doesn't match

### Source of the Issue
The character 'r' comes from the default mock response in `server/protocols/native/connection.go`:

```go
default:
    // Default response for unknown queries
    return MockResponse{
        columns: []MockColumn{{name: "result", dataType: "String"}},
        rows:    []MockRow{{fmt.Sprintf("Mock response for: %s", query)}},
    }
```

The column name "result" contains the letter 'r' which is being interpreted as a BlockInfo field number.

## Mock Queries Supported by Icebox

Based on the server implementation, the following mock queries are supported:

1. `SELECT 1` - Returns UInt8 value 1
2. `SELECT 42` - Returns UInt8 value 42  
3. `SELECT 'hello'` - Returns String value "hello"
4. `SELECT now()` - Returns current DateTime
5. `SELECT count(*)` - Returns UInt64 value 100
6. `SELECT * FROM users` - Returns mock user data (id, name, email, created_at)
7. `SELECT * FROM orders` - Returns mock order data (order_id, customer_id, amount, status)
8. Unknown queries - Returns default response with "result" column

## Recommendations

### Immediate Fixes
1. **Protocol Compliance**: Review and fix the native protocol implementation to match ClickHouse's expected format
2. **BlockInfo Handling**: Ensure proper BlockInfo field handling in data packets
3. **Packet Structure**: Verify that all required protocol fields are sent in the correct order

### Long-term Improvements
1. **Protocol Documentation**: Reference the official ClickHouse native protocol specification
2. **Compatibility Testing**: Implement automated tests against real ClickHouse servers
3. **Error Handling**: Improve error messages to help with debugging protocol issues

## Conclusion

While the icebox server successfully accepts connections from clickhouse-client, there are significant protocol compatibility issues that prevent successful query execution. The server needs protocol implementation fixes to achieve full compatibility with the ClickHouse client.

**Compatibility Status:** ❌ **Not Compatible** (Connection works, but protocol errors prevent query execution)

## Files Generated
- Test script: `test_clickhouse_compatibility.sh`
- Debug script: `debug_protocol.go` 
- Test output: `/tmp/icebox_clickhouse_compatibility_test_20250802_225836.log`
- This report: `clickhouse_compatibility_report.md` 