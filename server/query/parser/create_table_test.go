package parser

import (
	"strings"
	"testing"
)

func TestCreateTableParsing(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
		checkFunc   func(*testing.T, *CreateTableStmt)
	}{
		{
			name: "Basic CREATE TABLE with STORAGE",
			sql:  "CREATE TABLE users (id int32, name string) STORAGE filesystem",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				if stmt.StorageEngine == nil {
					t.Fatal("StorageEngine should be set")
				}
				if stmt.StorageEngine.Value != "filesystem" {
					t.Errorf("Expected storage engine 'filesystem', got '%s'", stmt.StorageEngine.Value)
				}
			},
		},
		{
			name: "CREATE TABLE with PARTITION BY",
			sql:  "CREATE TABLE events (id int32, date date, user_id int32) STORAGE filesystem PARTITION BY (date, user_id)",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.PartitionBy) != 2 {
					t.Fatalf("Expected 2 partition columns, got %d", len(stmt.PartitionBy))
				}
				if stmt.PartitionBy[0].Value != "date" {
					t.Errorf("Expected first partition column 'date', got '%s'", stmt.PartitionBy[0].Value)
				}
				if stmt.PartitionBy[1].Value != "user_id" {
					t.Errorf("Expected second partition column 'user_id', got '%s'", stmt.PartitionBy[1].Value)
				}
			},
		},
		{
			name: "CREATE TABLE with ORDER BY",
			sql:  "CREATE TABLE logs (id int32, timestamp timestamp, level string) STORAGE memory ORDER BY (timestamp, level)",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				if len(stmt.OrderBy) != 2 {
					t.Fatalf("Expected 2 order columns, got %d", len(stmt.OrderBy))
				}
				if stmt.OrderBy[0].Value != "timestamp" {
					t.Errorf("Expected first order column 'timestamp', got '%s'", stmt.OrderBy[0].Value)
				}
				if stmt.OrderBy[1].Value != "level" {
					t.Errorf("Expected second order column 'level', got '%s'", stmt.OrderBy[1].Value)
				}
			},
		},
		{
			name: "CREATE TABLE with SETTINGS",
			sql:  "CREATE TABLE temp_data (id int32, data string) STORAGE memory SETTINGS cache_enabled=true, batch_size=50000",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				if stmt.Settings == nil {
					t.Fatal("Settings should be set")
				}
				if cacheEnabled, exists := stmt.Settings["cache_enabled"]; !exists {
					t.Error("cache_enabled setting should exist")
				} else if cacheEnabled != true {
					t.Errorf("Expected cache_enabled=true, got %v", cacheEnabled)
				}
				if batchSize, exists := stmt.Settings["batch_size"]; !exists {
					t.Error("batch_size setting should exist")
				} else if batchSize != 50000 {
					t.Errorf("Expected batch_size=50000, got %v", batchSize)
				}
			},
		},
		{
			name: "CREATE TABLE with all clauses",
			sql:  "CREATE TABLE analytics (id int32, date date, metric string, value float64) STORAGE s3 PARTITION BY (date) ORDER BY (metric) SETTINGS compression=gzip, cache_size=1000",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				// Check storage
				if stmt.StorageEngine.Value != "s3" {
					t.Errorf("Expected storage engine 's3', got '%s'", stmt.StorageEngine.Value)
				}
				// Check partitioning
				if len(stmt.PartitionBy) != 1 || stmt.PartitionBy[0].Value != "date" {
					t.Error("Partitioning should be set to date")
				}
				// Check ordering
				if len(stmt.OrderBy) != 1 || stmt.OrderBy[0].Value != "metric" {
					t.Error("Ordering should be set to metric")
				}
				// Check settings
				if stmt.Settings["compression"] != "gzip" {
					t.Error("compression setting should be gzip")
				}
				if stmt.Settings["cache_size"] != 1000 {
					t.Error("cache_size setting should be 1000")
				}
			},
		},
		{
			name:        "CREATE TABLE without STORAGE (should fail)",
			sql:         "CREATE TABLE invalid_table (id int32)",
			expectError: true,
		},
		{
			name:        "CREATE TABLE with invalid STORAGE engine",
			sql:         "CREATE TABLE invalid_table (id int32) STORAGE invalid_engine",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Add semicolon if not present
			sql := tt.sql
			if sql[len(sql)-1] != ';' {
				sql += ";"
			}

			lexer := NewLexer([]byte(sql))
			parser := NewParser(lexer)

			node, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			createStmt, ok := node.(*CreateTableStmt)
			if !ok {
				t.Fatalf("Expected *CreateTableStmt, got %T", node)
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t, createStmt)
			}
		})
	}
}

func TestCreateTableClauseParsing(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
		description string
	}{
		{
			name:        "Valid STORAGE clause",
			sql:         "CREATE TABLE test (id int32) STORAGE filesystem;",
			expectError: false,
			description: "Should parse STORAGE clause correctly",
		},
		{
			name:        "Valid PARTITION BY clause",
			sql:         "CREATE TABLE test (id int32, date date) STORAGE memory PARTITION BY (date);",
			expectError: false,
			description: "Should parse PARTITION BY clause correctly",
		},
		{
			name:        "Valid ORDER BY clause",
			sql:         "CREATE TABLE test (id int32, name string) STORAGE filesystem ORDER BY (name);",
			expectError: false,
			description: "Should parse ORDER BY clause correctly",
		},
		{
			name:        "Valid SETTINGS clause",
			sql:         "CREATE TABLE test (id int32) STORAGE memory SETTINGS cache_enabled=true;",
			expectError: false,
			description: "Should parse SETTINGS clause correctly",
		},
		{
			name:        "Multiple clauses in correct order",
			sql:         "CREATE TABLE test (id int32, date date) STORAGE s3 PARTITION BY (date) ORDER BY (id) SETTINGS compression=gzip;",
			expectError: false,
			description: "Should parse multiple clauses correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer([]byte(tt.sql))
			parser := NewParser(lexer)

			_, err := parser.Parse()
			if tt.expectError && err == nil {
				t.Errorf("Expected error but got none: %s", tt.description)
			} else if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v - %s", err, tt.description)
			}
		})
	}
}
func TestCreateTableIcebergTypes(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
		errorMsg    string
		checkFunc   func(*testing.T, *CreateTableStmt)
	}{
		{
			name: "Basic Iceberg primitive types",
			sql:  "CREATE TABLE test (id int32, name string, active boolean, score float64) STORAGE filesystem",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				expectedTypes := map[string]string{
					"id":     "int32",
					"name":   "string",
					"active": "boolean",
					"score":  "float64",
				}
				for colName, expectedType := range expectedTypes {
					if colDef, exists := stmt.TableSchema.ColumnDefinitions[colName]; !exists {
						t.Errorf("Column '%s' should exist", colName)
					} else if colDef.DataType != expectedType {
						t.Errorf("Column '%s' should have type '%s', got '%s'", colName, expectedType, colDef.DataType)
					}
				}
			},
		},
		{
			name: "Iceberg decimal with precision and scale",
			sql:  "CREATE TABLE test (price decimal(10,2), rate decimal(5,4)) STORAGE filesystem",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				priceCol := stmt.TableSchema.ColumnDefinitions["price"]
				if priceCol.DataType != "decimal(10,2)" {
					t.Errorf("Price column should have type 'decimal(10,2)', got '%s'", priceCol.DataType)
				}
				if priceCol.Precision != 10 || priceCol.Scale != 2 {
					t.Errorf("Price column should have precision=10, scale=2, got precision=%d, scale=%d", priceCol.Precision, priceCol.Scale)
				}
			},
		},
		{
			name: "Iceberg timestamp types",
			sql:  "CREATE TABLE test (created_at timestamp, updated_at timestamptz) STORAGE filesystem",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				createdCol := stmt.TableSchema.ColumnDefinitions["created_at"]
				updatedCol := stmt.TableSchema.ColumnDefinitions["updated_at"]
				if createdCol.DataType != "timestamp" {
					t.Errorf("created_at should be 'timestamp', got '%s'", createdCol.DataType)
				}
				if updatedCol.DataType != "timestamptz" {
					t.Errorf("updated_at should be 'timestamptz', got '%s'", updatedCol.DataType)
				}
			},
		},
		{
			name: "Iceberg binary and UUID types",
			sql:  "CREATE TABLE test (data binary, user_id uuid) STORAGE filesystem",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				dataCol := stmt.TableSchema.ColumnDefinitions["data"]
				userIdCol := stmt.TableSchema.ColumnDefinitions["user_id"]
				if dataCol.DataType != "binary" {
					t.Errorf("data should be 'binary', got '%s'", dataCol.DataType)
				}
				if userIdCol.DataType != "uuid" {
					t.Errorf("user_id should be 'uuid', got '%s'", userIdCol.DataType)
				}
			},
		},
		{
			name:        "Legacy SQL types should be rejected - VARCHAR",
			sql:         "CREATE TABLE test (name VARCHAR(255)) STORAGE filesystem",
			expectError: true,
			errorMsg:    "unsupported SQL type 'VARCHAR'. Use Iceberg type 'string' instead",
		},
		{
			name:        "Legacy SQL types should be rejected - INT",
			sql:         "CREATE TABLE test (id INT) STORAGE filesystem",
			expectError: true,
			errorMsg:    "unsupported SQL type 'INT'. Use Iceberg type 'int32' instead",
		},
		{
			name:        "Legacy SQL types should be rejected - BIGINT",
			sql:         "CREATE TABLE test (id BIGINT) STORAGE filesystem",
			expectError: true,
			errorMsg:    "unsupported SQL type 'BIGINT'. Use Iceberg type 'int64' instead",
		},
		{
			name:        "Legacy SQL types should be rejected - DOUBLE",
			sql:         "CREATE TABLE test (score DOUBLE) STORAGE filesystem",
			expectError: true,
			errorMsg:    "unsupported SQL type 'DOUBLE'. Use Iceberg type 'float64' instead",
		},
		{
			name:        "Invalid Iceberg type",
			sql:         "CREATE TABLE test (id invalid_type) STORAGE filesystem",
			expectError: true,
			errorMsg:    "invalid Iceberg type 'invalid_type'",
		},
		{
			name:        "Duplicate column names",
			sql:         "CREATE TABLE test (id int32, id string) STORAGE filesystem",
			expectError: true,
			errorMsg:    "duplicate column name 'id'",
		},
		{
			name:        "No columns specified",
			sql:         "CREATE TABLE test () STORAGE filesystem",
			expectError: true,
			errorMsg:    "table must have at least one column",
		},
		{
			name: "Iceberg types with constraints",
			sql:  "CREATE TABLE test (id int32 NOT NULL, name string DEFAULT 'unknown', active boolean) STORAGE filesystem",
			checkFunc: func(t *testing.T, stmt *CreateTableStmt) {
				idCol := stmt.TableSchema.ColumnDefinitions["id"]
				nameCol := stmt.TableSchema.ColumnDefinitions["name"]

				if !idCol.NotNull || idCol.IsNullable {
					t.Error("id column should be NOT NULL")
				}
				if nameCol.DefaultValue != "unknown" {
					t.Errorf("name column should have default 'unknown', got '%s'", nameCol.DefaultValue)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Add semicolon if not present
			sql := tt.sql
			if sql[len(sql)-1] != ';' {
				sql += ";"
			}

			lexer := NewLexer([]byte(sql))
			parser := NewParser(lexer)

			node, err := parser.Parse()
			if tt.expectError {
				if err == nil {
					t.Error("Expected error but got none")
					return
				}
				if tt.errorMsg != "" && !strings.Contains(err.Error(), tt.errorMsg) {
					t.Errorf("Expected error message to contain '%s', got '%s'", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			createStmt, ok := node.(*CreateTableStmt)
			if !ok {
				t.Fatalf("Expected *CreateTableStmt, got %T", node)
			}

			if tt.checkFunc != nil {
				tt.checkFunc(t, createStmt)
			}
		})
	}
}

func TestIcebergTypeValidation(t *testing.T) {
	tests := []struct {
		name        string
		typeStr     string
		expectValid bool
		description string
	}{
		{
			name:        "Valid primitive types",
			typeStr:     "string",
			expectValid: true,
			description: "string should be valid",
		},
		{
			name:        "Valid int32",
			typeStr:     "int32",
			expectValid: true,
			description: "int32 should be valid",
		},
		{
			name:        "Valid decimal with precision/scale",
			typeStr:     "decimal(10,2)",
			expectValid: true,
			description: "decimal(10,2) should be valid",
		},
		{
			name:        "Invalid legacy SQL type",
			typeStr:     "VARCHAR",
			expectValid: false,
			description: "VARCHAR should be invalid (legacy SQL)",
		},
		{
			name:        "Invalid type name",
			typeStr:     "invalid_type",
			expectValid: false,
			description: "invalid_type should be invalid",
		},
		{
			name:        "Valid boolean",
			typeStr:     "boolean",
			expectValid: true,
			description: "boolean should be valid",
		},
		{
			name:        "Valid timestamp",
			typeStr:     "timestamp",
			expectValid: true,
			description: "timestamp should be valid",
		},
		{
			name:        "Valid timestamptz",
			typeStr:     "timestamptz",
			expectValid: true,
			description: "timestamptz should be valid",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid := IsValidDataType(tt.typeStr)
			if isValid != tt.expectValid {
				t.Errorf("Expected IsValidDataType('%s') = %v, got %v - %s",
					tt.typeStr, tt.expectValid, isValid, tt.description)
			}
		})
	}
}
