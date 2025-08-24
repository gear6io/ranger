# SQL Parser Integration with Ranger

This directory contains the SQL parser that has been integrated with the Ranger project. The parser provides comprehensive SQL parsing capabilities while maintaining compatibility with Ranger's architecture.

## Overview

The SQL parser is a production-ready SQL parser that supports:

- **ANSI SQL**: Standard SQL statements and syntax
- **DDL**: Data Definition Language (CREATE, DROP, ALTER, etc.)
- **DML**: Data Manipulation Language (SELECT, INSERT, UPDATE, DELETE)
- **DCL**: Data Control Language (GRANT, REVOKE)
- **TCL**: Transaction Control Language (BEGIN, COMMIT, ROLLBACK)
- **Modern Features**: Advanced SQL constructs, functions, and expressions

## Integration Details

### 1. **Replaces SQL parser-specific imports** with Ranger-compatible types
- **Import Paths**: Changed from external parser packages to local type definitions
- **Type Compatibility**: Maintains full parser functionality while using Ranger types
- **No External Dependencies**: Self-contained within the Ranger project

### 2. **Preserved SQL Parser Features**
- **Lexer**: Complete tokenization of SQL input
- **Parser**: Full AST generation for all supported statements
- **AST Nodes**: Comprehensive representation of SQL structures
- **Error Handling**: Robust error reporting and recovery
- **Performance**: Optimized parsing algorithms

## Usage Examples

### Basic Parsing
```go
package main

import "github.com/gear6io/ranger/server/query/parser"

func main() {
    input := []byte("SELECT * FROM users WHERE id = 1")
    lexer := parser.NewLexer(input)
    p := parser.NewParser(lexer)
    
    node, err := p.Parse()
    if err != nil {
        panic(err)
    }
    
    // Use the parsed AST node
    if selectStmt, ok := node.(*parser.SelectStmt); ok {
        // Process SELECT statement
    }
}
```

### Statement Analysis
```go
// Parse CREATE TABLE statement
input := []byte("CREATE TABLE users (id INT, name VARCHAR(255))")
lexer := parser.NewLexer(input)
p := parser.NewParser(lexer)

node, err := p.Parse()
if err != nil {
    panic(err)
}

if createStmt, ok := node.(*parser.CreateTableStmt); ok {
    tableName := createStmt.TableName.Value
    // Process table schema
}
```

## Supported Statements

### DDL Statements
- `CREATE TABLE` - Table creation with full schema support
- `CREATE INDEX` - Index creation
- `CREATE DATABASE` - Database creation
- `DROP TABLE` - Table removal
- `DROP INDEX` - Index removal
- `DROP DATABASE` - Database removal
- `ALTER TABLE` - Table modification

### DML Statements
- `SELECT` - Query data with complex expressions
- `INSERT` - Insert data into tables
- `UPDATE` - Modify existing data
- `DELETE` - Remove data from tables

### DCL Statements
- `GRANT` - Grant privileges
- `REVOKE` - Revoke privileges
- `CREATE USER` - User management
- `DROP USER` - User removal

### TCL Statements
- `BEGIN` - Start transaction
- `COMMIT` - Commit transaction
- `ROLLBACK` - Rollback transaction

## Testing

Run the parser tests:
```bash
go test ./server/query/parser/
```

## Future Plans

1. **Preserve SQL Parser Functionality**: Don't remove parsing capabilities unless absolutely necessary
2. **Performance Optimization**: Enhance parsing speed for large queries
3. **Extended SQL Support**: Add support for additional SQL dialects
4. **Integration Testing**: Comprehensive testing with Ranger components

## Architecture

The parser follows a standard lexer/parser architecture:
- **Lexer**: Tokenizes input SQL into tokens
- **Parser**: Builds Abstract Syntax Tree (AST) from tokens
- **AST Nodes**: Represent SQL structures in Go types
- **Error Handling**: Comprehensive error reporting

## Licensing

The SQL parser is licensed under the GNU Affero General Public License v3.0. See the original SQL parser project for full license details.

## Credits

- **SQL Parser Team**: For the comprehensive SQL parser implementation
