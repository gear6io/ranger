# Bug Report: SQL Query Table Reference Syntax Error

## Issue Description

When using the `icebox sql` command with dot notation for table references (e.g., `SELECT * FROM default.sales`), the query fails with a syntax error. This occurs because DuckDB, the underlying SQL engine, does not support the standard SQL dot notation for table references.

## Steps to Reproduce

1. Import a parquet file into a table:

```bash
icebox import data.parquet --table sales
```

2. Try to query the table using dot notation:

```bash
icebox sql 'SELECT * FROM default.sales LIMIT 10'
```

## Current Behavior

The query fails with the error:

```
Error: ❌ Query failed: failed to execute query [query_1748463466615817000_0]: Parser Error: syntax error at or near "default"
```

## Expected Behavior

The query should execute successfully, either by:

- Accepting the dot notation and converting it internally
- Accepting the underscore notation (`default_sales`)
- Accepting just the table name (`sales`)

## Impact

Users familiar with standard SQL syntax may encounter errors when trying to query tables using the common dot notation format. This creates a confusing user experience, especially since the table is displayed with dot notation in other command outputs.

## Technical Details

- Component: SQL Query Engine (DuckDB)
- File: `icebox/engine/duckdb/engine.go`
- Related Files: `icebox/cli/sql.go`

📁 Detected file format: parquet
📋 Schema inferred from data.parquet:

Columns (1):

1. d: date (nullable)

📊 File Statistics:
Records: 36
File size: 670 B
Columns: 1

📥 Importing data.parquet (parquet) into table [default sales]...
✅ Created namespace: [default]
✅ Created table: [default sales]
📁 Copied data to: file:///Users/antonio/Desktop/RESEARCH/EXPERIMENTS/ICEBOX/icebox/my-lakehouse/.icebox/data/default/sales
✅ Successfully imported table!

📊 Import Results:
Table: [default sales]
Records: 36
Size: 670 B
Location: file:///Users/antonio/Desktop/RESEARCH/EXPERIMENTS/ICEBOX/icebox/my-lakehouse/.icebox/data/default/sales

🚀 Next steps:
icebox sql 'SELECT *FROM default.sales LIMIT 10'
antonio@mac my-lakehouse % icebox sql 'SELECT* FROM default.sales LIMIT 10'
2025/05/28 16:17:45 Info: httpfs extension loaded successfully
2025/05/28 16:17:45 Info: iceberg extension loaded successfully
2025/05/28 16:17:45 Info: Unknown catalog type - using direct file access
2025/05/28 16:17:45 DuckDB engine initialized successfully with catalog: my-lakehouse
🔍 Found 1 namespaces: [[default]]
🔍 Checking namespace 'default' for tables...
🔍 Found table: default.sales
2025/05/28 16:17:46 Info: Created alias 'sales' -> 'default_sales'
2025/05/28 16:17:46 Registered table default_sales in 1.409335584s using DuckDB v1.3.0 native Iceberg support
✅ Successfully registered table: default.sales
📋 Registered 1 tables for querying
Error: ❌ Query failed: failed to execute query [query_1748463466615817000_0]: Parser Error: syntax error at or near "default"
Usage:
icebox sql [query] [flags]

Flags:
--auto-register automatically register catalog tables (default true)
--format string output format: table, csv, json (default "table")
-h, --help help for sql
--max-rows int maximum number of rows to display (default 1000)
--metrics show engine performance metrics after query
--show-schema show column schema information
--timing show query execution time (default true)

Global Flags:
-v, --verbose verbose output

Error: ❌ Query failed: failed to execute query [query_1748463466615817000_0]: Parser Error: syntax error at or near "default"
The file is called date.parquet in the repo, not data.parquet.
Problem is solved by using 'sales' instead of 'default.sales'. It's also ok to use 'default_sales' as this is what DuckDB uses.

Activity
antoniobadia
antoniobadia commented 3 days ago
antoniobadia
3 days ago
Author
Oops. I just realized the file in testdata (date.parquet) is not the one mentioned in the README (data.parquet). Sorry about that -although the problem mentioned may still happen.
