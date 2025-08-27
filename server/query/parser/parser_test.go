// Package parser tests
// Ranger SQL parser tests
// Copyright (C) Alex Gaetano Padula
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.
package parser

import (
	"strings"
	"testing"
)

// TestNewParserCreateDatabase tests CREATE DATABASE statement parsing with various scenarios
func TestNewParserCreateDatabase(t *testing.T) {
	tests := []struct {
		name              string
		statement         string
		description       string
		expectIfNotExists bool
	}{
		{
			name:              "BasicCreateDatabase",
			statement:         `CREATE DATABASE TEST;`,
			description:       "Basic CREATE DATABASE without IF NOT EXISTS",
			expectIfNotExists: false,
		},
		{
			name:              "CreateDatabaseIfNotExists",
			statement:         `CREATE DATABASE IF NOT EXISTS TEST;`,
			description:       "CREATE DATABASE with IF NOT EXISTS clause",
			expectIfNotExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)
			if parser == nil {
				t.Fatal("expected non-nil parser")
			}

			stmt, err := parser.Parse()
			if err != nil {
				t.Fatal(err)
			}

			if stmt == nil {
				t.Fatal("expected non-nil statement")
			}

			createDatabaseStmt, ok := stmt.(*CreateDatabaseStmt)
			if !ok {
				t.Fatalf("expected *CreateDatabaseStmt, got %T", stmt)
			}

			// Verify database name
			if createDatabaseStmt.Name.Value != "TEST" {
				t.Fatalf("expected TEST, got %s", createDatabaseStmt.Name.Value)
			}

			// Verify IF NOT EXISTS flag
			if createDatabaseStmt.IfNotExists != tt.expectIfNotExists {
				t.Fatalf("expected IfNotExists to be %v, got %v", tt.expectIfNotExists, createDatabaseStmt.IfNotExists)
			}

			t.Logf("✅ %s parsed successfully", tt.description)
		})
	}
}

// TestNewParserUseDatabase tests USE DATABASE statement parsing
func TestNewParserUseDatabase(t *testing.T) {
	statement := []byte(`
	USE TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	useDatabaseStmt, ok := stmt.(*UseStmt)
	if !ok {
		t.Fatalf("expected *UseStmt, got %T", stmt)
	}

	if useDatabaseStmt.DatabaseName.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", useDatabaseStmt.DatabaseName.Value)
	}
}

// TestNewParserCreateTable tests CREATE TABLE statement parsing with various scenarios
func TestNewParserCreateTable(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		shouldFail  bool
		expectedErr string
		description string
	}{
		{
			name:        "BasicTableWithoutStorage",
			statement:   `CREATE TABLE TEST (col1 INT, col2 CHAR(255), deci DECIMAL(10, 2));`,
			shouldFail:  true,
			expectedErr: "STORAGE clause is required for CREATE TABLE statements",
			description: "Should fail because STORAGE clause is mandatory",
		},
		{
			name:        "TableWithConstraintsWithoutStorage",
			statement:   `CREATE TABLE TEST (col1 INT SEQUENCE NOT NULL UNIQUE, col2 CHAR(255) UNIQUE, deci DECIMAL(10, 2));`,
			shouldFail:  true,
			expectedErr: "STORAGE clause is required for CREATE TABLE statements",
			description: "Should fail because STORAGE clause is mandatory even with constraints",
		},
		{
			name: "TableWithMemoryStorage",
			statement: `CREATE TABLE testdb.my_memory_table (
				id INT PRIMARY KEY,
				name VARCHAR(255),
				value DECIMAL(10, 2)
			) STORAGE memory;`,
			shouldFail:  false,
			description: "Should pass with MEMORY storage",
		},
		{
			name: "TableWithFilesystemStorage",
			statement: `CREATE TABLE testdb.my_filesystem_table (
				id INT PRIMARY KEY,
				description TEXT,
				amount DOUBLE
			) STORAGE filesystem;`,
			shouldFail:  false,
			description: "Should pass with FILESYSTEM storage",
		},
		{
			name: "TableWithIfNotExists",
			statement: `CREATE TABLE IF NOT EXISTS testdb.my_memory_table (
				id INT PRIMARY KEY,
				name VARCHAR(255),
				value DECIMAL(10, 2)
			) STORAGE memory;`,
			shouldFail:  false,
			description: "Should pass with IF NOT EXISTS clause",
		},
		{
			name: "TableWithIfNotExistsAndSimpleName",
			statement: `CREATE TABLE IF NOT EXISTS simple_table (
				id INT PRIMARY KEY,
				name VARCHAR(255)
			) STORAGE memory;`,
			shouldFail:  false,
			description: "Should pass with IF NOT EXISTS and simple table name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)
			if parser == nil {
				t.Fatal("expected non-nil parser")
			}

			stmt, err := parser.Parse()

			if tt.shouldFail {
				// Test should fail
				if err == nil {
					t.Fatal("expected error but got none")
				}
				if !strings.Contains(err.Error(), tt.expectedErr) {
					t.Fatalf("expected error containing '%s', got '%s'", tt.expectedErr, err.Error())
				}
				t.Logf("✅ Correctly failed with error: %v", err)
			} else {
				// Test should pass
				if err != nil {
					t.Fatal(err)
				}

				if stmt == nil {
					t.Fatal("expected non-nil statement")
				}

				createTableStmt, ok := stmt.(*CreateTableStmt)
				if !ok {
					t.Fatalf("expected *CreateTableStmt, got %T", stmt)
				}

				// Verify STORAGE clause is present
				if createTableStmt.StorageEngine == nil {
					t.Fatal("expected STORAGE clause to be parsed")
				}

				// Verify table name and storage engine
				if tt.name == "TableWithMemoryStorage" {
					if !createTableStmt.TableName.IsQualified() {
						t.Fatalf("expected qualified table name for TableWithMemoryStorage")
					}
					if createTableStmt.TableName.GetFullName() != "testdb.my_memory_table" {
						t.Fatalf("expected testdb.my_memory_table, got %s", createTableStmt.TableName.GetFullName())
					}
					if createTableStmt.StorageEngine.Value != "memory" {
						t.Fatalf("expected STORAGE memory, got STORAGE %s", createTableStmt.StorageEngine.Value)
					}
					if createTableStmt.IfNotExists {
						t.Fatalf("expected IfNotExists to be false for TableWithMemoryStorage")
					}
				} else if tt.name == "TableWithFilesystemStorage" {
					if !createTableStmt.TableName.IsQualified() {
						t.Fatalf("expected qualified table name for TableWithFilesystemStorage")
					}
					if createTableStmt.TableName.GetFullName() != "testdb.my_filesystem_table" {
						t.Fatalf("expected testdb.my_filesystem_table, got %s", createTableStmt.TableName.GetFullName())
					}
					if createTableStmt.StorageEngine.Value != "filesystem" {
						t.Fatalf("expected STORAGE filesystem, got STORAGE %s", createTableStmt.StorageEngine.Value)
					}
					if createTableStmt.IfNotExists {
						t.Fatalf("expected IfNotExists to be false for TableWithFilesystemStorage")
					}
				} else if tt.name == "TableWithIfNotExists" {
					if !createTableStmt.TableName.IsQualified() {
						t.Fatalf("expected qualified table name for TableWithIfNotExists")
					}
					if createTableStmt.TableName.GetFullName() != "testdb.my_memory_table" {
						t.Fatalf("expected testdb.my_memory_table, got %s", createTableStmt.TableName.GetFullName())
					}
					if createTableStmt.StorageEngine.Value != "memory" {
						t.Fatalf("expected STORAGE memory, got STORAGE %s", createTableStmt.StorageEngine.Value)
					}
					if !createTableStmt.IfNotExists {
						t.Fatalf("expected IfNotExists to be true for TableWithIfExists")
					}
				} else if tt.name == "TableWithIfNotExistsAndSimpleName" {
					if createTableStmt.TableName.IsQualified() {
						t.Fatalf("expected unqualified table name for TableWithIfNotExistsAndSimpleName")
					}
					if createTableStmt.TableName.Table.Value != "simple_table" {
						t.Fatalf("expected simple_table, got %s", createTableStmt.TableName.Table.Value)
					}
					if createTableStmt.StorageEngine.Value != "memory" {
						t.Fatalf("expected STORAGE memory, got STORAGE %s", createTableStmt.StorageEngine.Value)
					}
					if !createTableStmt.IfNotExists {
						t.Fatalf("expected IfNotExists to be true for TableWithIfNotExistsAndSimpleName")
					}
				}

				t.Logf("✅ %s parsed successfully", tt.description)
			}
		})
	}
}

// TestNewParserCreateIndex tests CREATE INDEX statement parsing
func TestNewParserCreateIndex(t *testing.T) {
	statement := []byte(`
	CREATE INDEX idx1 ON TEST (col1);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createIndexStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected *CreateIndexStmt, got %T", stmt)
	}

	if createIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.TableName.Table.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createIndexStmt.TableName.Table.Value)
	}

	if createIndexStmt.ColumnNames[0].Value != "col1" {
		t.Fatalf("expected col1, got %s", createIndexStmt.ColumnNames[0].Value)
	}
}

// TestNewParserCreateIndexUnique tests CREATE UNIQUE INDEX statement parsing
func TestNewParserCreateIndexUnique(t *testing.T) {
	statement := []byte(`
	CREATE UNIQUE INDEX idx1 ON TEST (col1);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createIndexStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected *CreateIndexStmt, got %T", stmt)
	}

	if createIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.TableName.Table.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createIndexStmt.TableName.Table.Value)
	}

	if createIndexStmt.ColumnNames[0].Value != "col1" {
		t.Fatalf("expected col1, got %s", createIndexStmt.ColumnNames[0].Value)
	}

	if !createIndexStmt.Unique {
		t.Fatalf("expected true, got %v", createIndexStmt.Unique)
	}
}

// TestNewParserCreateIndexMultipleColumns tests CREATE INDEX with multiple columns
func TestNewParserCreateIndexMultipleColumns(t *testing.T) {
	statement := []byte(`
	CREATE UNIQUE INDEX idx1 ON TEST (col1, col2, col3);
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createIndexStmt, ok := stmt.(*CreateIndexStmt)
	if !ok {
		t.Fatalf("expected *CreateIndexStmt, got %T", stmt)
	}

	if createIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", createIndexStmt.IndexName.Value)
	}

	if createIndexStmt.TableName.Table.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", createIndexStmt.TableName.Table.Value)
	}

	expectedColumns := []string{"col1", "col2", "col3"}
	for i, col := range createIndexStmt.ColumnNames {
		if col.Value != expectedColumns[i] {
			t.Fatalf("expected %s, got %s", expectedColumns[i], col.Value)
		}
	}

	if !createIndexStmt.Unique {
		t.Fatalf("expected true, got %v", createIndexStmt.Unique)
	}
}

// TestNewParserInsert tests INSERT statement parsing with various scenarios
func TestNewParserInsert(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		description string
	}{
		{
			name:        "BasicInsert",
			statement:   `INSERT INTO TEST (col1, col2) VALUES (1, 'hello'), (2, 'world');`,
			description: "Basic INSERT with simple table name",
		},
		{
			name: "InsertWithQualifiedTable",
			statement: `INSERT INTO testdb.my_memory_table (id, name, value) VALUES 
				(1, 'Test User 1', 100.50),
				(2, 'Test User 2', 200.75);`,
			description: "INSERT with qualified table name (database.table)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)
			if parser == nil {
				t.Fatal("expected non-nil parser")
			}

			stmt, err := parser.Parse()
			if err != nil {
				t.Fatal(err)
			}

			if stmt == nil {
				t.Fatal("expected non-nil statement")
			}

			insertStmt, ok := stmt.(*InsertStmt)
			if !ok {
				t.Fatalf("expected *InsertStmt, got %T", stmt)
			}

			// Verify table name
			if tt.name == "BasicInsert" {
				if insertStmt.TableName.Table.Value != "TEST" {
					t.Fatalf("expected TEST, got %s", insertStmt.TableName.Table.Value)
				}
				if insertStmt.TableName.IsQualified() {
					t.Fatalf("expected unqualified table name for BasicInsert")
				}
			} else if tt.name == "InsertWithQualifiedTable" {
				if !insertStmt.TableName.IsQualified() {
					t.Fatalf("expected qualified table name for InsertWithQualifiedTable")
				}
				if insertStmt.TableName.GetFullName() != "testdb.my_memory_table" {
					t.Fatalf("expected testdb.my_memory_table, got %s", insertStmt.TableName.GetFullName())
				}
			}

			// Verify column names
			if tt.name == "BasicInsert" {
				if len(insertStmt.ColumnNames) != 2 {
					t.Fatalf("expected 2 columns, got %d", len(insertStmt.ColumnNames))
				}
				if insertStmt.ColumnNames[0].Value != "col1" {
					t.Fatalf("expected first column to be 'col1', got %s", insertStmt.ColumnNames[0].Value)
				}
				if insertStmt.ColumnNames[1].Value != "col2" {
					t.Fatalf("expected second column to be 'col2', got %s", insertStmt.ColumnNames[1].Value)
				}
			} else if tt.name == "InsertWithQualifiedTable" {
				if len(insertStmt.ColumnNames) != 3 {
					t.Fatalf("expected 3 columns, got %d", len(insertStmt.ColumnNames))
				}
				if insertStmt.ColumnNames[0].Value != "id" {
					t.Fatalf("expected first column to be 'id', got %s", insertStmt.ColumnNames[0].Value)
				}
				if insertStmt.ColumnNames[1].Value != "name" {
					t.Fatalf("expected second column to be 'name', got %s", insertStmt.ColumnNames[1].Value)
				}
				if insertStmt.ColumnNames[2].Value != "value" {
					t.Fatalf("expected third column to be 'value', got %s", insertStmt.ColumnNames[2].Value)
				}
			}

			// Verify values
			if tt.name == "BasicInsert" {
				if len(insertStmt.Values) != 2 {
					t.Fatalf("expected 2 value rows, got %d", len(insertStmt.Values))
				}
			} else if tt.name == "InsertWithQualifiedTable" {
				if len(insertStmt.Values) != 2 {
					t.Fatalf("expected 2 value rows, got %d", len(insertStmt.Values))
				}
			}

			t.Logf("✅ %s parsed successfully", tt.description)
		})
	}
}

// TestNewParserDropDatabase tests DROP DATABASE statement parsing
func TestNewParserDropDatabase(t *testing.T) {
	statement := []byte(`
	DROP DATABASE TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	dropDatabaseStmt, ok := stmt.(*DropDatabaseStmt)
	if !ok {
		t.Fatalf("expected *DropDatabaseStmt, got %T", stmt)
	}

	if dropDatabaseStmt.Name.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", dropDatabaseStmt.Name.Value)
	}
}

// TestNewParserDropTable tests DROP TABLE statement parsing with various scenarios
func TestNewParserDropTable(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		shouldFail  bool
		expectedErr string
		description string
	}{
		{
			name:        "BasicDropTable",
			statement:   `DROP TABLE TEST;`,
			shouldFail:  false,
			description: "Basic DROP TABLE with simple table name",
		},
		{
			name:        "DropTableWithQualifiedName",
			statement:   `DROP TABLE testdb.my_table;`,
			shouldFail:  false,
			description: "DROP TABLE with qualified table name (database.table) - should now parse correctly",
		},
		{
			name:        "DropTableIfExists",
			statement:   `DROP TABLE IF EXISTS TEST;`,
			shouldFail:  false,
			description: "DROP TABLE with IF EXISTS clause - should now parse correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statement := []byte(tt.statement)
			t.Log(tt.description)
			t.Log(string(statement))

			lexer := NewLexer(statement)
			parser := NewParser(lexer)
			if parser == nil {
				t.Fatal("expected non-nil parser")
			}

			stmt, err := parser.Parse()

			if tt.shouldFail {
				// Test should fail
				if err == nil {
					t.Fatal("expected error but got none")
				}
				if !strings.Contains(err.Error(), tt.expectedErr) {
					t.Fatalf("expected error containing '%s', got '%s'", tt.expectedErr, err.Error())
				}
				t.Logf("✅ Correctly failed with error: %v", err)
			} else {
				// Test should pass
				if err != nil {
					t.Fatal(err)
				}

				if stmt == nil {
					t.Fatal("expected non-nil statement")
				}

				dropTableStmt, ok := stmt.(*DropTableStmt)
				if !ok {
					t.Fatalf("expected *DropTableStmt, got %T", stmt)
				}

				// Verify table name based on test case
				if tt.name == "BasicDropTable" {
					if dropTableStmt.TableName.Table.Value != "TEST" {
						t.Fatalf("expected TEST, got %s", dropTableStmt.TableName.Table.Value)
					}
					if dropTableStmt.TableName.IsQualified() {
						t.Fatalf("expected unqualified table name, but got qualified")
					}
				} else if tt.name == "DropTableWithQualifiedName" {
					// Should now properly parse qualified names
					if !dropTableStmt.TableName.IsQualified() {
						t.Fatalf("expected qualified table name, but got unqualified")
					}
					if dropTableStmt.TableName.Database.Value != "testdb" {
						t.Fatalf("expected database 'testdb', got %s", dropTableStmt.TableName.Database.Value)
					}
					if dropTableStmt.TableName.Table.Value != "my_table" {
						t.Fatalf("expected table 'my_table', got %s", dropTableStmt.TableName.Table.Value)
					}
					t.Logf("✅ Qualified table name 'testdb.my_table' parsed correctly")
				} else if tt.name == "DropTableIfExists" {
					// Should now properly parse IF EXISTS
					if dropTableStmt.TableName.Table.Value != "TEST" {
						t.Fatalf("expected TEST, got %s", dropTableStmt.TableName.Table.Value)
					}
					if !dropTableStmt.IfExists {
						t.Fatalf("expected IfExists to be true")
					}
					t.Logf("✅ IF EXISTS clause parsed correctly")
				}

				t.Logf("✅ %s parsed successfully", tt.description)
			}
		})
	}
}

// TestNewParserDropIndex tests DROP INDEX statement parsing
func TestNewParserDropIndex(t *testing.T) {
	statement := []byte(`
	DROP INDEX idx1 ON TEST;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	dropIndexStmt, ok := stmt.(*DropIndexStmt)
	if !ok {
		t.Fatalf("expected *DropIndexStmt, got %T", stmt)
	}

	if dropIndexStmt.IndexName.Value != "idx1" {
		t.Fatalf("expected idx1, got %s", dropIndexStmt.IndexName.Value)
	}

	if dropIndexStmt.TableName.Table.Value != "TEST" {
		t.Fatalf("expected TEST, got %s", dropIndexStmt.TableName.Table.Value)
	}
}

// TestNewParserSelectSimple tests simple SELECT statement parsing
func TestNewParserSelectSimple(t *testing.T) {
	statement := []byte(`
	SELECT 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*Literal).Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %v", selectStmt.SelectList.Expressions[0].Value.(*Literal).Value)
	}
}

// TestNewParserSelectWithArithmetic tests SELECT with arithmetic expressions
func TestNewParserSelectWithArithmetic(t *testing.T) {
	statement := []byte(`
	SELECT 1+1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	// Note: This test may need adjustment based on how binary expressions are parsed
	// The exact structure depends on the parser implementation
}

// TestNewParserSelectColumns tests SELECT with column names
func TestNewParserSelectColumns(t *testing.T) {
	statement := []byte(`
	SELECT col1, col2, col3;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 3 {
		t.Fatalf("expected 3 expressions, got %d", len(selectStmt.SelectList.Expressions))
	}

	expectedColumns := []string{"col1", "col2", "col3"}
	for i, col := range selectStmt.SelectList.Expressions {
		if col.Value.(*ColumnSpecification).ColumnName.Value != expectedColumns[i] {
			t.Fatalf("expected %s, got %s", expectedColumns[i], col.Value.(*ColumnSpecification).ColumnName.Value)
		}
	}
}

// TestNewParserSelectFromTable tests SELECT with FROM clause
func TestNewParserSelectFromTable(t *testing.T) {
	statement := []byte(`
	SELECT col1 FROM tbl1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if selectStmt.SelectList == nil {
		t.Fatal("expected non-nil SelectList")
	}

	if len(selectStmt.SelectList.Expressions) != 1 {
		t.Fatalf("expected 1 expression, got %d", len(selectStmt.SelectList.Expressions))
	}

	if selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value != "col1" {
		t.Fatalf("expected col1, got %s", selectStmt.SelectList.Expressions[0].Value.(*ColumnSpecification).ColumnName.Value)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}
}

// TestNewParserSelectWithWhere tests SELECT with WHERE clause
func TestNewParserSelectWithWhere(t *testing.T) {
	statement := []byte(`
	SELECT col1, col2 FROM tbl1 WHERE col1 <> 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("expected *SelectStmt, got %T", stmt)
	}

	if selectStmt.TableExpression.FromClause.Tables[0].Name.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", selectStmt.TableExpression.FromClause.Tables[0].Name.Value)
	}

	// Note: This test may need adjustment based on how comparison predicates are parsed
	// The exact structure depends on the parser implementation
}

// TestNewParserUpdate tests UPDATE statement parsing
func TestNewParserUpdate(t *testing.T) {
	statement := []byte(`
	UPDATE tbl1 SET col1 = 1 WHERE col2 = 2;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	updateStmt, ok := stmt.(*UpdateStmt)
	if !ok {
		t.Fatalf("expected *UpdateStmt, got %T", stmt)
	}

	if updateStmt.TableName.Table.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", updateStmt.TableName.Table.Value)
	}

	if updateStmt.SetClause[0].Column.Value != "col1" {
		t.Fatalf("expected col1, got %s", updateStmt.SetClause[0].Column.Value)
	}

	if updateStmt.SetClause[0].Value.Value.(uint64) != uint64(1) {
		t.Fatalf("expected 1, got %v", updateStmt.SetClause[0].Value.Value)
	}
}

// TestNewParserDelete tests DELETE statement parsing
func TestNewParserDelete(t *testing.T) {
	statement := []byte(`
	DELETE FROM tbl1 WHERE col1 = 1;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	deleteStmt, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("expected *DeleteStmt, got %T", stmt)
	}

	if deleteStmt.TableName.Table.Value != "tbl1" {
		t.Fatalf("expected tbl1, got %s", deleteStmt.TableName.Table.Value)
	}
}

// TestNewParserBegin tests BEGIN statement parsing
func TestNewParserBegin(t *testing.T) {
	statement := []byte(`
	BEGIN;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	beginStmt, ok := stmt.(*BeginStmt)
	if !ok {
		t.Fatalf("expected *BeginStmt, got %T", stmt)
	}

	if beginStmt == nil {
		t.Fatal("expected non-nil statement")
	}
}

// TestNewParserRollback tests ROLLBACK statement parsing
func TestNewParserRollback(t *testing.T) {
	statement := []byte(`
	ROLLBACK;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	rollbackStmt, ok := stmt.(*RollbackStmt)
	if !ok {
		t.Fatalf("expected *RollbackStmt, got %T", stmt)
	}

	if rollbackStmt == nil {
		t.Fatal("expected non-nil statement")
	}
}

// TestNewParserCommit tests COMMIT statement parsing
func TestNewParserCommit(t *testing.T) {
	statement := []byte(`
	COMMIT;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	commitStmt, ok := stmt.(*CommitStmt)
	if !ok {
		t.Fatalf("expected *CommitStmt, got %T", stmt)
	}

	if commitStmt == nil {
		t.Fatal("expected non-nil statement")
	}
}

// TestNewParserCreateUser tests CREATE USER statement parsing
func TestNewParserCreateUser(t *testing.T) {
	statement := []byte(`
	CREATE USER username IDENTIFIED BY 'password';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	createUserStmt, ok := stmt.(*CreateUserStmt)
	if !ok {
		t.Fatalf("expected *CreateUserStmt, got %T", stmt)
	}

	if createUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if createUserStmt.Username.Value != "username" {
		t.Fatalf("expected username, got %s", createUserStmt.Username.Value)
	}

	if createUserStmt.Password.Value != "password" {
		t.Fatalf("expected password, got %s", createUserStmt.Password.Value)
	}
}

// TestNewParserGrant tests GRANT statement parsing
func TestNewParserGrant(t *testing.T) {
	statement := []byte(`
	GRANT CONNECT TO username;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	grantStmt, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}

	if grantStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if grantStmt.PrivilegeDefinition.Actions[0] != PRIV_CONNECT {
		t.Fatalf("expected CONNECT, got %v", grantStmt.PrivilegeDefinition.Actions[0])
	}

	if grantStmt.PrivilegeDefinition.Grantee.Value != "username" {
		t.Fatalf("expected username, got %s", grantStmt.PrivilegeDefinition.Grantee.Value)
	}
}

// TestNewParserGrantMultiplePrivileges tests GRANT with multiple privileges
func TestNewParserGrantMultiplePrivileges(t *testing.T) {
	statement := []byte(`
	GRANT SELECT, CREATE, DROP ON db1.* TO username;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	grantStmt, ok := stmt.(*GrantStmt)
	if !ok {
		t.Fatalf("expected *GrantStmt, got %T", stmt)
	}

	if grantStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	expectedActions := []PrivilegeAction{PRIV_SELECT, PRIV_CREATE, PRIV_DROP}
	for i, action := range grantStmt.PrivilegeDefinition.Actions {
		if action != expectedActions[i] {
			t.Fatalf("expected %v, got %v", expectedActions[i], action)
		}
	}

	if grantStmt.PrivilegeDefinition.Grantee.Value != "username" {
		t.Fatalf("expected username, got %s", grantStmt.PrivilegeDefinition.Grantee.Value)
	}

	if grantStmt.PrivilegeDefinition.Object.Value != "db1.*" {
		t.Fatalf("expected db1.*, got %s", grantStmt.PrivilegeDefinition.Object.Value)
	}
}

// TestNewParserRevoke tests REVOKE statement parsing
func TestNewParserRevoke(t *testing.T) {
	statement := []byte(`
	REVOKE CREATE, DROP ON db1.* FROM username;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	revokeStmt, ok := stmt.(*RevokeStmt)
	if !ok {
		t.Fatalf("expected *RevokeStmt, got %T", stmt)
	}

	if revokeStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	expectedActions := []PrivilegeAction{PRIV_CREATE, PRIV_DROP}
	for i, action := range revokeStmt.PrivilegeDefinition.Actions {
		if action != expectedActions[i] {
			t.Fatalf("expected %v, got %v", expectedActions[i], action)
		}
	}

	if revokeStmt.PrivilegeDefinition.Revokee.Value != "username" {
		t.Fatalf("expected username, got %s", revokeStmt.PrivilegeDefinition.Revokee.Value)
	}

	if revokeStmt.PrivilegeDefinition.Object.Value != "db1.*" {
		t.Fatalf("expected db1.*, got %s", revokeStmt.PrivilegeDefinition.Object.Value)
	}
}

// TestNewParserDropUser tests DROP USER statement parsing
func TestNewParserDropUser(t *testing.T) {
	statement := []byte(`
	DROP USER username;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	dropUserStmt, ok := stmt.(*DropUserStmt)
	if !ok {
		t.Fatalf("expected *DropUserStmt, got %T", stmt)
	}

	if dropUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if dropUserStmt.Username.Value != "username" {
		t.Fatalf("expected username, got %s", dropUserStmt.Username.Value)
	}
}

// TestNewParserShowDatabases tests SHOW DATABASES statement parsing
func TestNewParserShowDatabases(t *testing.T) {
	statement := []byte(`
	SHOW DATABASES;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if showStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showStmt.ShowType != SHOW_DATABASES {
		t.Fatalf("expected SHOW_DATABASES, got %v", showStmt.ShowType)
	}
}

// TestNewParserShowTables tests SHOW TABLES statement parsing
func TestNewParserShowTables(t *testing.T) {
	statement := []byte(`
	SHOW TABLES;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if showStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showStmt.ShowType != SHOW_TABLES {
		t.Fatalf("expected SHOW_TABLES, got %v", showStmt.ShowType)
	}
}

// TestNewParserShowUsers tests SHOW USERS statement parsing
func TestNewParserShowUsers(t *testing.T) {
	statement := []byte(`
	SHOW USERS;
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	showStmt, ok := stmt.(*ShowStmt)
	if !ok {
		t.Fatalf("expected *ShowStmt, got %T", stmt)
	}

	if showStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if showStmt.ShowType != SHOW_USERS {
		t.Fatalf("expected SHOW_USERS, got %v", showStmt.ShowType)
	}
}

// TestDatabaseTableParsing tests parsing of database.table format
func TestDatabaseTableParsing(t *testing.T) {
	// Test parsing of database.table format
	query := "SELECT * FROM db1.table1;"

	// Create a new parser
	lexer := NewLexer([]byte(query))
	parser := NewParser(lexer)

	// Parse the query
	result, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Check that it's a SELECT statement
	selectStmt, ok := result.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", result)
	}

	// Check that the FROM clause has the correct table structure
	if selectStmt.TableExpression == nil {
		t.Fatal("Expected TableExpression to be set")
	}
	if selectStmt.TableExpression.FromClause == nil {
		t.Fatal("Expected FromClause to be set")
	}
	if len(selectStmt.TableExpression.FromClause.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(selectStmt.TableExpression.FromClause.Tables))
	}

	table := selectStmt.TableExpression.FromClause.Tables[0]
	if table.Database == nil {
		t.Fatal("Database should be set for db1.table1")
	}
	if table.Name == nil {
		t.Fatal("Table name should be set for db1.table1")
	}

	if table.Database.Value != "db1" {
		t.Errorf("Expected database 'db1', got '%s'", table.Database.Value)
	}
	if table.Name.Value != "table1" {
		t.Errorf("Expected table 'table1', got '%s'", table.Name.Value)
	}
}

// TestSimpleTableParsing tests parsing of simple table name (no database)
func TestSimpleTableParsing(t *testing.T) {
	// Test parsing of simple table name (no database)
	query := "SELECT * FROM table1;"

	// Create a new parser
	lexer := NewLexer([]byte(query))
	parser := NewParser(lexer)

	// Parse the query
	result, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Check that it's a SELECT statement
	selectStmt, ok := result.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", result)
	}

	// Check that the FROM clause has the correct table structure
	if selectStmt.TableExpression == nil {
		t.Fatal("Expected TableExpression to be set")
	}
	if selectStmt.TableExpression.FromClause == nil {
		t.Fatal("Expected FromClause to be set")
	}
	if len(selectStmt.TableExpression.FromClause.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(selectStmt.TableExpression.FromClause.Tables))
	}

	table := selectStmt.TableExpression.FromClause.Tables[0]
	if table.Database != nil {
		t.Fatal("Database should be nil for simple table name")
	}
	if table.Name == nil {
		t.Fatal("Table name should be set")
	}

	if table.Name.Value != "table1" {
		t.Errorf("Expected table 'table1', got '%s'", table.Name.Value)
	}
}

// TestTableWithAliasParsing tests parsing of database.table with alias
func TestTableWithAliasParsing(t *testing.T) {
	// Test parsing of database.table with alias
	query := "SELECT * FROM db1.table1 AS t1;"

	// Create a new parser
	lexer := NewLexer([]byte(query))
	parser := NewParser(lexer)

	// Parse the query
	result, err := parser.Parse()
	if err != nil {
		t.Fatalf("Failed to parse query: %v", err)
	}

	// Check that it's a SELECT statement
	selectStmt, ok := result.(*SelectStmt)
	if !ok {
		t.Fatalf("Expected SelectStmt, got %T", result)
	}

	// Check that the FROM clause has the correct table structure
	if selectStmt.TableExpression == nil {
		t.Fatal("Expected TableExpression to be set")
	}
	if selectStmt.TableExpression.FromClause == nil {
		t.Fatal("Expected FromClause to be set")
	}
	if len(selectStmt.TableExpression.FromClause.Tables) != 1 {
		t.Fatalf("Expected 1 table, got %d", len(selectStmt.TableExpression.FromClause.Tables))
	}

	table := selectStmt.TableExpression.FromClause.Tables[0]
	if table.Database == nil {
		t.Fatal("Database should be set for db1.table1")
	}
	if table.Name == nil {
		t.Fatal("Table name should be set for db1.table1")
	}
	if table.Alias == nil {
		t.Fatal("Alias should be set")
	}

	if table.Database.Value != "db1" {
		t.Errorf("Expected database 'db1', got '%s'", table.Database.Value)
	}
	if table.Name.Value != "table1" {
		t.Errorf("Expected table 'table1', got '%s'", table.Name.Value)
	}
	if table.Alias.Value != "t1" {
		t.Errorf("Expected alias 't1', got '%s'", table.Alias.Value)
	}
}

// TestNewParserAlterUser tests ALTER USER statement parsing
func TestNewParserAlterUser(t *testing.T) {
	statement := []byte(`
	ALTER USER admin SET PASSWORD 'newpassword';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	alterUserStmt, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}

	if alterUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if alterUserStmt.Username.Value != "admin" {
		t.Fatalf("expected admin, got %s", alterUserStmt.Username.Value)
	}

	if alterUserStmt.Value.Value != "newpassword" {
		t.Fatalf("expected newpassword, got %s", alterUserStmt.Value.Value)
	}

	if alterUserStmt.SetType != ALTER_USER_SET_PASSWORD {
		t.Fatalf("expected PASSWORD, got %v", alterUserStmt.SetType)
	}
}

// TestNewParserAlterUserUsername tests ALTER USER with username change
func TestNewParserAlterUserUsername(t *testing.T) {
	statement := []byte(`
	ALTER USER admin SET USERNAME 'newusername';
`)

	lexer := NewLexer(statement)
	t.Log(string(statement))

	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	stmt, err := parser.Parse()
	if err != nil {
		t.Fatal(err)
	}

	if stmt == nil {
		t.Fatal("expected non-nil statement")
	}

	alterUserStmt, ok := stmt.(*AlterUserStmt)
	if !ok {
		t.Fatalf("expected *AlterUserStmt, got %T", stmt)
	}

	if alterUserStmt == nil {
		t.Fatal("expected non-nil statement")
	}

	if alterUserStmt.Username.Value != "admin" {
		t.Fatalf("expected admin, got %s", alterUserStmt.Username.Value)
	}

	if alterUserStmt.Value.Value != "newusername" {
		t.Fatalf("expected newusername, got %s", alterUserStmt.Value.Value)
	}

	if alterUserStmt.SetType != ALTER_USER_SET_USERNAME {
		t.Fatalf("expected USERNAME, got %v", alterUserStmt.SetType)
	}
}

// TestNewParserEmptyStatement tests parsing of empty statements
func TestNewParserEmptyStatement(t *testing.T) {
	statement := []byte(``)

	lexer := NewLexer(statement)
	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	_, err := parser.Parse()
	if err == nil {
		t.Fatal("expected error for empty statement")
	}

	if err.Error() != "empty statement" {
		t.Fatalf("expected 'empty statement' error, got: %s", err.Error())
	}
}

// TestNewParserMissingSemicolon tests parsing of statements without semicolon
func TestNewParserMissingSemicolon(t *testing.T) {
	statement := []byte(`SELECT 1`)

	lexer := NewLexer(statement)
	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	_, err := parser.Parse()
	if err == nil {
		t.Fatal("expected error for missing semicolon")
	}

	if err.Error() != "expected ';'" {
		t.Fatalf("expected 'expected ;' error, got: %s", err.Error())
	}
}

// TestNewParserInvalidKeyword tests parsing of statements with invalid keywords
func TestNewParserInvalidKeyword(t *testing.T) {
	statement := []byte(`INVALID KEYWORD;`)

	lexer := NewLexer(statement)
	parser := NewParser(lexer)
	if parser == nil {
		t.Fatal("expected non-nil parser")
	}

	_, err := parser.Parse()
	if err == nil {
		t.Fatal("expected error for invalid keyword")
	}

	if err.Error() != "expected keyword" {
		t.Fatalf("expected 'expected keyword' error, got: %s", err.Error())
	}
}

// TestNewParserLexerBasic tests basic lexer functionality
func TestNewParserLexerBasic(t *testing.T) {
	statement := []byte(`SELECT 1;`)
	lexer := NewLexer(statement)

	if lexer == nil {
		t.Fatal("expected non-nil lexer")
	}

	if lexer.input == nil {
		t.Fatal("expected non-nil input")
	}

	if string(lexer.input) != "SELECT 1;" {
		t.Fatalf("expected 'SELECT 1;', got: %s", string(lexer.input))
	}
}

// TestNewParserLexerTokenization tests lexer tokenization
func TestNewParserLexerTokenization(t *testing.T) {
	statement := []byte(`SELECT 1;`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Check that we have the expected tokens
	expectedTokens := []TokenType{KEYWORD_TOK, LITERAL_TOK, SEMICOLON_TOK}
	if len(lexer.tokens) != len(expectedTokens) {
		t.Fatalf("expected %d tokens, got %d", len(expectedTokens), len(lexer.tokens))
	}

	for i, expectedType := range expectedTokens {
		if lexer.tokens[i].tokenT != expectedType {
			t.Fatalf("token %d: expected type %v, got %v", i, expectedType, lexer.tokens[i].tokenT)
		}
	}
}

// TestNewParserLexerComments tests lexer comment handling
func TestNewParserLexerComments(t *testing.T) {
	statement := []byte(`-- This is a comment
SELECT 1;`)
	lexer := NewLexer(statement)

	lexer.tokenize()
	lexer.stripComments()

	// After stripping comments, we should have fewer tokens
	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens after stripping comments")
	}

	// Check that comment tokens were removed
	for _, token := range lexer.tokens {
		if token.tokenT == COMMENT_TOK {
			t.Fatal("expected no comment tokens after stripping")
		}
	}
}

// TestNewParserLexerStringLiterals tests lexer string literal handling
func TestNewParserLexerStringLiterals(t *testing.T) {
	statement := []byte(`SELECT 'hello world';`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Find the string literal token
	var stringToken *Token
	for i := range lexer.tokens {
		if lexer.tokens[i].tokenT == LITERAL_TOK {
			stringToken = &lexer.tokens[i]
			break
		}
	}

	if stringToken == nil {
		t.Fatal("expected to find string literal token")
	}

	if stringToken.value != "'hello world'" {
		t.Fatalf("expected 'hello world', got: %s", stringToken.value)
	}
}

// TestNewParserLexerNumbers tests lexer number handling
func TestNewParserLexerNumbers(t *testing.T) {
	statement := []byte(`SELECT 123, 45.67;`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Find the number tokens
	var numberTokens []*Token
	for i := range lexer.tokens {
		if lexer.tokens[i].tokenT == LITERAL_TOK {
			numberTokens = append(numberTokens, &lexer.tokens[i])
		}
	}

	if len(numberTokens) < 2 {
		t.Fatal("expected at least 2 number tokens")
	}

	// Check first number (integer)
	if numberTokens[0].value != uint64(123) {
		t.Fatalf("expected 123, got: %v", numberTokens[0].value)
	}

	// Check second number (float)
	if numberTokens[1].value != 45.67 {
		t.Fatalf("expected 45.67, got: %v", numberTokens[1].value)
	}
}

// TestNewParserLexerIdentifiers tests lexer identifier handling
func TestNewParserLexerIdentifiers(t *testing.T) {
	statement := []byte(`SELECT column_name, table_name;`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Find the identifier tokens
	var identifierTokens []*Token
	for i := range lexer.tokens {
		if lexer.tokens[i].tokenT == IDENT_TOK {
			identifierTokens = append(identifierTokens, &lexer.tokens[i])
		}
	}

	if len(identifierTokens) < 2 {
		t.Fatal("expected at least 2 identifier tokens")
	}

	// Check first identifier
	if identifierTokens[0].value != "column_name" {
		t.Fatalf("expected column_name, got: %s", identifierTokens[0].value)
	}

	// Check second identifier
	if identifierTokens[1].value != "table_name" {
		t.Fatalf("expected table_name, got: %s", identifierTokens[1].value)
	}
}

// TestNewParserLexerKeywords tests lexer keyword handling
func TestNewParserLexerKeywords(t *testing.T) {
	// Test single-word keywords
	statement := []byte(`SELECT DISTINCT FROM WHERE;`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Find the keyword tokens
	var keywordTokens []*Token
	for i := range lexer.tokens {
		if lexer.tokens[i].tokenT == KEYWORD_TOK {
			keywordTokens = append(keywordTokens, &lexer.tokens[i])
		}
	}

	if len(keywordTokens) < 4 {
		t.Fatal("expected at least 4 keyword tokens")
	}

	expectedKeywords := []string{"SELECT", "DISTINCT", "FROM", "WHERE"}
	for i, expected := range expectedKeywords {
		if keywordTokens[i].value != expected {
			t.Fatalf("expected %s, got: %s", expected, keywordTokens[i].value)
		}
	}

	// Test multi-word keywords like "IF NOT EXISTS"
	multiWordStatement := []byte(`CREATE DATABASE IF NOT EXISTS testdb;`)
	multiWordLexer := NewLexer(multiWordStatement)
	multiWordLexer.tokenize()

	// Find the IF NOT EXISTS token
	foundIfNotExists := false
	for _, token := range multiWordLexer.tokens {
		if token.tokenT == KEYWORD_TOK && token.value == "IF NOT EXISTS" {
			foundIfNotExists = true
			break
		}
	}

	if !foundIfNotExists {
		t.Fatal("Expected to find 'IF NOT EXISTS' as a single keyword token")
	}

	t.Log("✅ Lexer correctly recognizes both single-word and multi-word keywords")
}

// TestNewParserLexerOperators tests lexer operator handling
func TestNewParserLexerOperators(t *testing.T) {
	statement := []byte(`1 + 2 * 3 / 4 % 5;`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Check that we have the expected operator tokens
	expectedOperators := []TokenType{PLUS_TOK, ASTERISK_TOK, DIVIDE_TOK, MODULUS_TOK}
	operatorCount := 0

	for _, token := range lexer.tokens {
		for _, expectedOp := range expectedOperators {
			if token.tokenT == expectedOp {
				operatorCount++
				break
			}
		}
	}

	if operatorCount < len(expectedOperators) {
		t.Fatalf("expected %d operators, found %d", len(expectedOperators), operatorCount)
	}
}

// TestNewParserLexerComparisonOperators tests lexer comparison operator handling
func TestNewParserLexerComparisonOperators(t *testing.T) {
	statement := []byte(`1 = 2 AND 3 <> 4 AND 5 < 6 AND 7 <= 8 AND 9 > 10 AND 11 >= 12;`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Check that we have comparison operator tokens
	comparisonCount := 0
	for _, token := range lexer.tokens {
		if token.tokenT == COMPARISON_TOK {
			comparisonCount++
		}
	}

	// We expect 6 comparison operators: =, <>, <, <=, >, >=
	if comparisonCount < 6 {
		t.Fatalf("expected at least 6 comparison operators, found %d", comparisonCount)
	}
}

// TestNewParserLexerParentheses tests lexer parentheses handling
func TestNewParserLexerParentheses(t *testing.T) {
	statement := []byte(`(1 + 2) * (3 - 4);`)
	lexer := NewLexer(statement)

	lexer.tokenize()

	if len(lexer.tokens) == 0 {
		t.Fatal("expected tokens to be generated")
	}

	// Check that we have left and right parentheses
	leftParenCount := 0
	rightParenCount := 0

	for _, token := range lexer.tokens {
		if token.tokenT == LPAREN_TOK {
			leftParenCount++
		} else if token.tokenT == RPAREN_TOK {
			rightParenCount++
		}
	}

	if leftParenCount != 2 {
		t.Fatalf("expected 2 left parentheses, found %d", leftParenCount)
	}

	if rightParenCount != 2 {
		t.Fatalf("expected 2 right parentheses, found %d", rightParenCount)
	}
}
