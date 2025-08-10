package parser

import (
	"testing"
)

// TestIceboxIntegration tests that the SQL parser integrates properly with Icebox
func TestIceboxIntegration(t *testing.T) {
	t.Run("Basic SQL Parsing", func(t *testing.T) {
		// Test basic SELECT statement
		input := []byte("SELECT * FROM users WHERE id = 1;")
		lexer := NewLexer(input)
		parser := NewParser(lexer)

		node, err := parser.Parse()
		if err != nil {
			t.Fatalf("Failed to parse basic SELECT: %v", err)
		}

		if node == nil {
			t.Fatal("Parser returned nil node")
		}

		// Verify it's a SelectStmt
		selectStmt, ok := node.(*SelectStmt)
		if !ok {
			t.Fatalf("Expected SelectStmt, got %T", node)
		}

		if selectStmt.TableExpression == nil {
			t.Fatal("SelectStmt missing TableExpression")
		}
	})

	t.Run("CREATE TABLE Statement", func(t *testing.T) {
		// Test CREATE TABLE statement
		input := []byte("CREATE TABLE users (id INT, name VARCHAR(255));")
		lexer := NewLexer(input)
		parser := NewParser(lexer)

		node, err := parser.Parse()
		if err != nil {
			t.Fatalf("Failed to parse CREATE TABLE: %v", err)
		}

		if node == nil {
			t.Fatal("Parser returned nil node")
		}

		// Verify it's a CreateTableStmt
		createStmt, ok := node.(*CreateTableStmt)
		if !ok {
			t.Fatalf("Expected CreateTableStmt, got %T", node)
		}

		if createStmt.TableName == nil {
			t.Fatal("CreateTableStmt missing TableName")
		}

		if createStmt.TableName.Value != "users" {
			t.Fatalf("Expected table name 'users', got '%s'", createStmt.TableName.Value)
		}
	})

	t.Run("INSERT Statement", func(t *testing.T) {
		// Test INSERT statement
		input := []byte("INSERT INTO users (id, name) VALUES (1, 'John');")
		lexer := NewLexer(input)
		parser := NewParser(lexer)

		node, err := parser.Parse()
		if err != nil {
			t.Fatalf("Failed to parse INSERT: %v", err)
		}

		if node == nil {
			t.Fatal("Parser returned nil node")
		}

		// Verify it's an InsertStmt
		insertStmt, ok := node.(*InsertStmt)
		if !ok {
			t.Fatalf("Expected InsertStmt, got %T", node)
		}

		if insertStmt.TableName == nil {
			t.Fatal("InsertStmt missing TableName")
		}

		if insertStmt.TableName.Value != "users" {
			t.Fatalf("Expected table name 'users', got '%s'", insertStmt.TableName.Value)
		}
	})

	t.Run("Complex SELECT with JOIN", func(t *testing.T) {
		// Test complex SELECT with JOIN
		input := []byte("SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.active = true;")
		lexer := NewLexer(input)
		parser := NewParser(lexer)

		node, err := parser.Parse()
		if err != nil {
			t.Fatalf("Failed to parse complex SELECT: %v", err)
		}

		if node == nil {
			t.Fatal("Parser returned nil node")
		}

		// Verify it's a SelectStmt
		selectStmt, ok := node.(*SelectStmt)
		if !ok {
			t.Fatalf("Expected SelectStmt, got %T", node)
		}

		if selectStmt.SelectList == nil {
			t.Fatal("SelectStmt missing SelectList")
		}
	})

	t.Run("Data Type Validation", func(t *testing.T) {
		// Test that our Icebox-compatible data types work
		validTypes := []string{"INT", "VARCHAR", "BOOLEAN", "TIMESTAMP", "UUID"}
		for _, dt := range validTypes {
			if !IsValidDataType(dt) {
				t.Errorf("Expected data type '%s' to be valid", dt)
			}
		}

		invalidTypes := []string{"INVALID_TYPE", "NOT_A_TYPE", "FAKE"}
		for _, dt := range invalidTypes {
			if IsValidDataType(dt) {
				t.Errorf("Expected data type '%s' to be invalid", dt)
			}
		}
	})

	t.Run("Privilege Actions", func(t *testing.T) {
		// Test that privilege actions are properly defined
		expectedPrivileges := []PrivilegeAction{
			PRIV_SELECT, PRIV_INSERT, PRIV_UPDATE, PRIV_DELETE,
			PRIV_CREATE, PRIV_DROP, PRIV_ALTER, PRIV_GRANT,
		}

		for _, priv := range expectedPrivileges {
			if priv == "" {
				t.Errorf("Privilege action should not be empty")
			}
		}
	})
}

// TestParserErrorHandling tests that the parser handles errors gracefully
func TestParserErrorHandling(t *testing.T) {
	t.Run("Invalid SQL", func(t *testing.T) {
		// Test with invalid SQL that should cause parsing errors
		invalidInputs := []string{
			"SELECT * FROM",             // Missing table name
			"CREATE TABLE",              // Missing table definition
			"INSERT INTO",               // Missing table and values
			"SELECT * FROM users WHERE", // Incomplete WHERE clause
		}

		for _, input := range invalidInputs {
			lexer := NewLexer([]byte(input))
			parser := NewParser(lexer)

			_, err := parser.Parse()
			if err == nil {
				t.Errorf("Expected error for invalid input: %s", input)
			}
		}
	})
}

// TestParserCompatibility tests that the parser maintains compatibility with SQL parser features
func TestParserCompatibility(t *testing.T) {
	t.Run("SQL Parser Keywords", func(t *testing.T) {
		// Test that basic SQL keywords are recognized by testing data types
		keywords := []string{"INT", "VARCHAR", "BOOLEAN", "TIMESTAMP", "UUID"}
		for _, keyword := range keywords {
			if !IsValidDataType(keyword) {
				t.Errorf("Expected SQL parser keyword '%s' to be recognized", keyword)
			}
		}
	})
}
