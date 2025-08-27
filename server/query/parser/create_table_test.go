package parser

import (
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
			sql:  "CREATE TABLE users (id INT, name VARCHAR(255)) STORAGE filesystem",
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
			sql:  "CREATE TABLE events (id INT, date DATE, user_id INT) STORAGE filesystem PARTITION BY (date, user_id)",
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
			sql:  "CREATE TABLE logs (id INT, timestamp TIMESTAMP, level VARCHAR(10)) STORAGE memory ORDER BY (timestamp, level)",
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
			sql:  "CREATE TABLE temp_data (id INT, data TEXT) STORAGE memory SETTINGS cache_enabled=true, batch_size=50000",
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
			sql:  "CREATE TABLE analytics (id INT, date DATE, metric VARCHAR(50), value DOUBLE) STORAGE s3 PARTITION BY (date) ORDER BY (metric) SETTINGS compression=gzip, cache_size=1000",
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
			sql:         "CREATE TABLE invalid_table (id INT)",
			expectError: true,
		},
		{
			name:        "CREATE TABLE with invalid STORAGE engine",
			sql:         "CREATE TABLE invalid_table (id INT) STORAGE invalid_engine",
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
			sql:         "CREATE TABLE test (id INT) STORAGE filesystem;",
			expectError: false,
			description: "Should parse STORAGE clause correctly",
		},
		{
			name:        "Valid PARTITION BY clause",
			sql:         "CREATE TABLE test (id INT, date DATE) STORAGE memory PARTITION BY (date);",
			expectError: false,
			description: "Should parse PARTITION BY clause correctly",
		},
		{
			name:        "Valid ORDER BY clause",
			sql:         "CREATE TABLE test (id INT, name VARCHAR(50)) STORAGE filesystem ORDER BY (name);",
			expectError: false,
			description: "Should parse ORDER BY clause correctly",
		},
		{
			name:        "Valid SETTINGS clause",
			sql:         "CREATE TABLE test (id INT) STORAGE memory SETTINGS cache_enabled=true;",
			expectError: false,
			description: "Should parse SETTINGS clause correctly",
		},
		{
			name:        "Multiple clauses in correct order",
			sql:         "CREATE TABLE test (id INT, date DATE) STORAGE s3 PARTITION BY (date) ORDER BY (id) SETTINGS compression=gzip;",
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
