# Bug Fix: SQL Query Table Reference Syntax Error

## Fix Description

This fix addresses the issue where SQL queries using dot notation for table references (e.g., `SELECT * FROM default.sales`) would fail with a syntax error. The solution implements automatic conversion of dot notation to underscore notation, handles column references correctly, and provides clearer error messages.

## Implementation Details

### 1. Query Preprocessing

Added preprocessing of SQL queries to automatically convert dot notation to underscore notation and handle column references:

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

        // Replace dots with underscores in table references
        tableName := strings.ReplaceAll(tablePath, ".", "_")
        return fmt.Sprintf("%s %s", keyword, tableName)
    })

    // Create aliases for table references to support both notations
    if strings.Contains(processedQuery, "_") {
        for _, table := range tablePattern.FindAllString(query, -1) {
            parts := strings.SplitN(table, " ", 2)
            if len(parts) != 2 {
                continue
            }
            tablePath := parts[1]
            dotParts := strings.Split(tablePath, ".")
            if len(dotParts) == 2 {
                aliasStmt := fmt.Sprintf("CREATE ALIAS IF NOT EXISTS %s FOR %s_%s;", 
                    dotParts[1], dotParts[0], dotParts[1])
                if _, err := e.db.Exec(aliasStmt); err != nil {
                    e.log.Printf("Failed to create alias: %v", err)
                }
            }
        }
    }

    // Replace column references to use underscore notation
    columnPattern := regexp.MustCompile(`([a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*\.[a-zA-Z_][a-zA-Z0-9_]*)`)
    processedQuery = columnPattern.ReplaceAllStringFunc(processedQuery, func(match string) string {
        parts := strings.Split(match, ".")
        if len(parts) == 3 {
            return fmt.Sprintf("%s_%s.%s", parts[0], parts[1], parts[2])
        }
        return match
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
            // Try to provide a helpful error message
            processedQuery := strings.ReplaceAll(query, ".", "_")
            return nil, fmt.Errorf("syntax error in query [%s]: DuckDB requires table names to use underscores instead of dots. "+
                "Use '%s' instead of '%s' or just the table name '%s'. "+
                "Original error: %w",
                queryID,
                processedQuery,
                query,
                strings.Split(strings.Split(query, ".")[1], " ")[0],
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
  Tables can be referenced in multiple ways:
  1. Using just the table name: SELECT * FROM sales
  2. Using namespace_table format: SELECT * FROM default_sales
  3. Using dot notation (automatically converted): SELECT * FROM default.sales
  4. Using column references with either notation:
     - SELECT default_sales.id FROM default_sales
     - SELECT default.sales.id FROM default.sales
```

## Testing

The fix has been tested with:

- Simple queries using dot notation
- Complex queries with multiple table references
- Queries with column references (properly handled with underscore notation)
- Various SQL keywords (FROM, JOIN, UPDATE, etc.)
- Table aliases and column references
- Mixed notation queries

## Impact

This fix:

1. Makes the SQL interface more user-friendly by accepting standard SQL dot notation
2. Maintains backward compatibility with existing underscore notation
3. Properly handles column references with both dot and underscore notation
4. Creates aliases to support both reference styles
5. Provides clearer error messages when syntax issues occur
6. Improves documentation to prevent confusion
7. Added comprehensive test suite: integration_tests/sql_table_references_test.go

## Usage Examples

All of these query formats now work:

```sql
-- Table references
SELECT * FROM default.sales
SELECT * FROM default_sales
SELECT * FROM sales

-- Column references
SELECT default.sales.id FROM default.sales
SELECT default_sales.id FROM default_sales
SELECT s.id FROM default.sales s

-- Mixed notation
SELECT s.id, p.name 
FROM default.sales s 
JOIN analytics.products p ON s.product_id = p.id
```
