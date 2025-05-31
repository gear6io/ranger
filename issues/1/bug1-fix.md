# Bug Fix: SQL Query Table Reference Syntax Error

## Fix Description

This fix addresses the issue where SQL queries using dot notation for table references (e.g., `SELECT * FROM default.sales`) would fail with a syntax error. The solution implements automatic conversion of dot notation to underscore notation and provides clearer error messages.

## Implementation Details

### 1. Query Preprocessing

Added preprocessing of SQL queries to automatically convert dot notation to underscore notation:

```go
func (e *Engine) preprocessQuery(ctx context.Context, query string) (string, error) {
    // Convert dot notation to underscore notation for table references
    tablePattern := regexp.MustCompile(`(FROM|JOIN|UPDATE|INTO|TABLE)\s+([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)`)
    
    processedQuery := tablePattern.ReplaceAllStringFunc(query, func(match string) string {
        parts := strings.SplitN(match, " ", 2)
        if len(parts) != 2 {
            return match
        }
        
        keyword := parts[0]
        tablePath := parts[1]
        
        // Replace dots with underscores in the table reference
        tableName := strings.ReplaceAll(tablePath, ".", "_")
        
        return keyword + " " + tableName
    })
    
    return processedQuery, nil
}
```

### 2. Enhanced Error Messages

Improved error handling to provide clearer guidance when syntax errors occur:

```go
func (e *Engine) executeSecureQuery(ctx context.Context, query, queryID string) (*sql.Rows, error) {
    rows, err := e.db.QueryContext(ctx, query)
    if err != nil {
        // Check for syntax error related to table references
        if strings.Contains(strings.ToLower(err.Error()), "syntax error") &&
            strings.Contains(query, ".") {
            return nil, fmt.Errorf("syntax error in query [%s]: DuckDB requires table names to use underscores instead of dots. "+
                "Use '%s' instead of '%s' or just the table name '%s'. "+
                "Original error: %w",
                queryID,
                strings.ReplaceAll(query, ".", "_"),
                query,
                strings.Split(query, ".")[1],
                err)
        }
        return nil, err
    }
    return rows, nil
}
```

### 3. Updated Documentation

Added clear documentation in the SQL command help text to explain table naming conventions:

```go
Table Naming:
  Tables can be referenced in two ways:
  1. Using just the table name: SELECT * FROM sales
  2. Using namespace_table format: SELECT * FROM default_sales
  Note: The dot notation (default.sales) is not supported by DuckDB.
```

## Testing

The fix has been tested with:

- Simple queries using dot notation
- Complex queries with multiple table references
- Queries with column references (which should not be affected)
- Various SQL keywords (FROM, JOIN, UPDATE, etc.)

## Impact

This fix:

1. Makes the SQL interface more user-friendly by accepting standard SQL dot notation
2. Maintains backward compatibility with existing underscore notation
3. Provides clearer error messages when syntax issues occur
4. Improves documentation to prevent confusion

## Usage Examples

All of these query formats now work:

```sql
SELECT * FROM default.sales
SELECT * FROM default_sales
SELECT * FROM sales
```
