package parser

import (
	"testing"
)

func TestAlterTableSettingsParsing(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
		errorMsg    string
	}{
		{
			name:        "Basic ALTER TABLE SETTINGS",
			sql:         "ALTER TABLE test_db.users SETTINGS cache_enabled=true",
			expectError: false,
		},
		{
			name:        "ALTER TABLE SETTINGS with multiple settings",
			sql:         "ALTER TABLE test_db.events SETTINGS cache_enabled=true, batch_size=50000",
			expectError: false,
		},
		{
			name:        "ALTER TABLE SETTINGS with different value types",
			sql:         "ALTER TABLE test_db.temp_data SETTINGS cache_size=1000, parallel_processing=true, compression=gzip",
			expectError: false,
		},
		{
			name:        "ALTER TABLE SETTINGS with numeric values",
			sql:         "ALTER TABLE analytics SETTINGS max_schema_versions=5, batch_validation_size=20000",
			expectError: false,
		},
		{
			name:        "ALTER TABLE SETTINGS missing equals",
			sql:         "ALTER TABLE test_db.users SETTINGS cache_enabled",
			expectError: true,
			errorMsg:    "expected = after setting key",
		},
		{
			name:        "ALTER TABLE SETTINGS missing value",
			sql:         "ALTER TABLE users SETTINGS cache_enabled=",
			expectError: true,
			errorMsg:    "expected value after =",
		},
		{
			name:        "ALTER TABLE SETTINGS with invalid syntax",
			sql:         "ALTER TABLE users SETTINGS =true",
			expectError: true,
			errorMsg:    "expected identifier after SETTINGS",
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
			result, err := parser.Parse()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				if tt.errorMsg != "" && err.Error() != tt.errorMsg {
					t.Errorf("Expected error message '%s', got '%s'", tt.errorMsg, err.Error())
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify the result is an AlterTableStmt
			alterStmt, ok := result.(*AlterTableStmt)
			if !ok {
				t.Errorf("Expected AlterTableStmt, got %T", result)
				return
			}

			// Verify the action type
			if alterStmt.Action != AlterTableActionSettings {
				t.Errorf("Expected action AlterTableActionSettings, got %v", alterStmt.Action)
			}

			// Verify settings are parsed
			if alterStmt.Settings == nil {
				t.Errorf("Expected settings to be parsed, got nil")
				return
			}

			// Verify specific settings based on the test case
			switch tt.name {
			case "Basic ALTER TABLE SETTINGS":
				if val, exists := alterStmt.Settings["cache_enabled"]; !exists || val != true {
					t.Errorf("Expected cache_enabled=true, got %v", val)
				}
			case "ALTER TABLE SETTINGS with multiple settings":
				if val, exists := alterStmt.Settings["cache_enabled"]; !exists || val != true {
					t.Errorf("Expected cache_enabled=true, got %v", val)
				}
				if val, exists := alterStmt.Settings["batch_size"]; !exists || val != 50000 {
					t.Errorf("Expected batch_size=50000, got %v", val)
				}
			case "ALTER TABLE SETTINGS with different value types":
				if val, exists := alterStmt.Settings["cache_size"]; !exists || val != 1000 {
					t.Errorf("Expected cache_size=1000, got %v", val)
				}
				if val, exists := alterStmt.Settings["parallel_processing"]; !exists || val != true {
					t.Errorf("Expected parallel_processing=true, got %v", val)
				}
				if val, exists := alterStmt.Settings["compression"]; !exists || val != "gzip" {
					t.Errorf("Expected compression=gzip, got %v", val)
				}
			}
		})
	}
}

func TestAlterTableExistingFunctionality(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		expectError bool
		action      AlterTableAction
	}{
		{
			name:        "ALTER TABLE DROP COLUMN",
			sql:         "ALTER TABLE users DROP COLUMN email",
			expectError: false,
			action:      AlterTableActionDropColumn,
		},
		{
			name:        "ALTER TABLE ALTER COLUMN",
			sql:         "ALTER TABLE users ALTER COLUMN name VARCHAR(255)",
			expectError: false,
			action:      AlterTableActionAlterColumn,
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
			result, err := parser.Parse()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error but got none")
					return
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
				return
			}

			// Verify the result is an AlterTableStmt
			alterStmt, ok := result.(*AlterTableStmt)
			if !ok {
				t.Errorf("Expected AlterTableStmt, got %T", result)
				return
			}

			// Verify the action type
			if alterStmt.Action != tt.action {
				t.Errorf("Expected action %v, got %v", tt.action, alterStmt.Action)
			}
		})
	}
}
