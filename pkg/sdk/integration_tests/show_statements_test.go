package integration_tests

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestShowDatabases tests SHOW DATABASES functionality
func TestShowDatabases(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Execute SHOW DATABASES
		result, err := client.Query(ctx, "SHOW DATABASES;")
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify result structure
		assert.True(t, len(result.Data) >= 0)
		assert.NotEmpty(t, result.Cols)

		// Should have at least the default database
		foundDefault := false
		for _, row := range result.Data {
			if len(row) > 0 {
				dbName, ok := row[0].(string)
				if ok && dbName == "default" {
					foundDefault = true
					break
				}
			}
		}
		assert.True(t, foundDefault, "Should find 'default' database in results")

		t.Logf("SHOW DATABASES returned %d rows", len(result.Data))
		for i, row := range result.Data {
			if i < 5 { // Log first 5 rows
				t.Logf("Row %d: %v", i, row)
			}
		}
	})
}

// TestShowTables tests SHOW TABLES functionality
func TestShowTables(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// First create a test database and table
		_, err := client.Query(ctx, "CREATE DATABASE IF NOT EXISTS testdb;")
		require.NoError(t, err)

		_, err = client.Query(ctx, `CREATE TABLE IF NOT EXISTS testdb.users (
		id int32 PRIMARY KEY,
		name string,
		email string
		) STORAGE FILESYSTEM;`)
		require.NoError(t, err)

		// Test SHOW TABLES without database
		result, err := client.Query(ctx, "SHOW TABLES;")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, len(result.Data) >= 0)

		// Test SHOW TABLES FROM specific database
		result, err = client.Query(ctx, "SHOW TABLES FROM testdb;")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, len(result.Data) >= 0)

		// Should find the users table we created
		foundUsers := false
		for _, row := range result.Data {
			if len(row) > 0 {
				tableName, ok := row[0].(string)
				if ok && tableName == "users" {
					foundUsers = true
					break
				}
			}
		}
		assert.True(t, foundUsers, "Should find 'users' table in testdb")

		t.Logf("SHOW TABLES FROM testdb returned %d rows", len(result.Data))
		for i, row := range result.Data {
			if i < 5 { // Log first 5 rows
				t.Logf("Row %d: %v", i, row)
			}
		}
	})
}

// TestShowColumns tests SHOW COLUMNS functionality
func TestShowColumns(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create test database and table with known columns
		_, err := client.Query(ctx, "CREATE DATABASE IF NOT EXISTS testdb;")
		require.NoError(t, err)

		_, err = client.Query(ctx, `CREATE TABLE IF NOT EXISTS testdb.users (
			id int32 PRIMARY KEY,
		name string NOT NULL,
		email string UNIQUE,
		age int32,
		created_at timestamp
		) STORAGE FILESYSTEM;`)
		require.NoError(t, err)

		// Test SHOW COLUMNS FROM table
		result, err := client.Query(ctx, "SHOW COLUMNS FROM testdb.users;")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, len(result.Data) >= 0)

		// Should have at least 5 columns (id, name, email, age, created_at)
		assert.True(t, len(result.Data) >= 5, "Should have at least 5 columns")

		// Verify column structure
		expectedColumns := []string{"id", "name", "email", "age", "created_at"}
		foundColumns := make(map[string]bool)

		for _, row := range result.Data {
			if len(row) > 0 {
				columnName, ok := row[0].(string)
				if ok {
					foundColumns[columnName] = true
				}
			}
		}

		for _, expectedCol := range expectedColumns {
			assert.True(t, foundColumns[expectedCol], "Should find column: %s", expectedCol)
		}

		t.Logf("SHOW COLUMNS FROM testdb.users returned %d rows", len(result.Data))
		for i, row := range result.Data {
			if i < 10 { // Log first 10 rows
				t.Logf("Row %d: %v", i, row)
			}
		}
	})
}

// TestShowCreateTable tests SHOW CREATE TABLE functionality
func TestShowCreateTable(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create test database and table
		_, err := client.Query(ctx, "CREATE DATABASE IF NOT EXISTS testdb;")
		require.NoError(t, err)

		_, err = client.Query(ctx, `CREATE TABLE IF NOT EXISTS testdb.users (
			id int32 PRIMARY KEY,
		name string NOT NULL,
		email string UNIQUE,
		age int32,
		created_at timestamp
		) STORAGE FILESYSTEM;`)
		require.NoError(t, err)

		// Test SHOW CREATE TABLE
		result, err := client.Query(ctx, "SHOW CREATE TABLE testdb.users;")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, len(result.Data) >= 0)

		// Should have at least one row with the DDL
		assert.True(t, len(result.Data) >= 1, "Should have at least one row with DDL")

		// Verify DDL contains expected elements
		if len(result.Data) > 0 && len(result.Data[0]) > 1 {
			ddl, ok := result.Data[0][1].(string)
			if ok {
				assert.Contains(t, ddl, "CREATE TABLE", "DDL should contain CREATE TABLE")
				assert.Contains(t, ddl, "testdb.users", "DDL should contain table name")
				assert.Contains(t, ddl, "id int32", "DDL should contain id column")
				assert.Contains(t, ddl, "name string", "DDL should contain name column")
				assert.Contains(t, ddl, "email string", "DDL should contain email column")
				assert.Contains(t, ddl, "age int32", "DDL should contain age column")
				assert.Contains(t, ddl, "created_at timestamp", "DDL should contain created_at column")
			}
		}

		t.Logf("SHOW CREATE TABLE testdb.users returned %d rows", len(result.Data))
		for i, row := range result.Data {
			t.Logf("Row %d: %v", i, row)
		}
	})
}

// TestShowUsers tests SHOW USERS functionality
func TestShowUsers(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Execute SHOW USERS
		result, err := client.Query(ctx, "SHOW USERS;")
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify result structure
		assert.True(t, len(result.Data) >= 0)
		assert.NotEmpty(t, result.Cols)

		t.Logf("SHOW USERS returned %d rows", len(result.Data))
		for i, row := range result.Data {
			if i < 5 { // Log first 5 rows
				t.Logf("Row %d: %v", i, row)
			}
		}
	})
}

// TestSystemDatabaseQueries tests SELECT queries against system database
func TestSystemDatabaseQueries(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create test database and table first
		_, err := client.Query(ctx, "CREATE DATABASE IF NOT EXISTS testdb;")
		require.NoError(t, err)

		_, err = client.Query(ctx, `CREATE TABLE IF NOT EXISTS testdb.users (
		id int32 PRIMARY KEY,
		name string,
		email string
		) STORAGE FILESYSTEM;`)
		require.NoError(t, err)

		// Test SELECT from system.databases
		result, err := client.Query(ctx, "SELECT * FROM system_databases;")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, len(result.Data) >= 0)

		// Test SELECT from system.tables
		result, err = client.Query(ctx, "SELECT * FROM system_tables WHERE database_name = 'testdb';")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, len(result.Data) >= 0)

		// Should find the users table
		foundUsers := false
		for _, row := range result.Data {
			if len(row) > 1 {
				tableName, ok := row[1].(string)
				if ok && tableName == "users" {
					foundUsers = true
					break
				}
			}
		}
		assert.True(t, foundUsers, "Should find 'users' table in system_tables")

		// Test SELECT from system.columns
		result, err = client.Query(ctx, "SELECT * FROM system_columns WHERE table_name = 'users' AND database_name = 'testdb';")
		require.NoError(t, err)
		require.NotNil(t, result)
		assert.True(t, len(result.Data) >= 0)

		// Should have at least 3 columns (id, name, email)
		assert.True(t, len(result.Data) >= 3, "Should have at least 3 columns")

		t.Logf("System database queries completed successfully")
	})
}

// TestShowStatementsErrorHandling tests error cases for SHOW statements
func TestShowStatementsErrorHandling(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Test SHOW COLUMNS with non-existent table
		_, err := client.Query(ctx, "SHOW COLUMNS FROM nonexistent_table;")
		assert.Error(t, err, "Should error for non-existent table")

		// Test SHOW CREATE TABLE with non-existent table
		_, err = client.Query(ctx, "SHOW CREATE TABLE nonexistent_table;")
		assert.Error(t, err, "Should error for non-existent table")

		// Test SHOW TABLES FROM non-existent database
		_, err = client.Query(ctx, "SHOW TABLES FROM nonexistent_database;")
		assert.Error(t, err, "Should error for non-existent database")

		t.Log("Error handling tests completed")
	})
}

// TestShowStatementsCaseInsensitive tests case insensitivity of SHOW statements
func TestShowStatementsCaseInsensitive(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		client := server.GetClient(t)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Create test database and table
		_, err := client.Query(ctx, "CREATE DATABASE IF NOT EXISTS testdb;")
		require.NoError(t, err)

		_, err = client.Query(ctx, `CREATE TABLE IF NOT EXISTS testdb.users (
			id int32 PRIMARY KEY,
			name string
		) STORAGE FILESYSTEM;`)
		require.NoError(t, err)

		// Test various case combinations
		testCases := []string{
			"show databases",
			"SHOW DATABASES",
			"Show Databases",
			"show tables from testdb",
			"SHOW TABLES FROM testdb;",
			"Show Tables From testdb",
			"show columns from testdb.users",
			"SHOW COLUMNS FROM testdb.users",
			"Show Columns From testdb.users",
			"show create table testdb.users",
			"SHOW CREATE TABLE testdb.users",
			"Show Create Table testdb.users",
		}

		for _, query := range testCases {
			t.Run(query, func(t *testing.T) {
				result, err := client.Query(ctx, query)
				assert.NoError(t, err, "Query should succeed: %s", query)
				assert.NotNil(t, result, "Result should not be nil: %s", query)
			})
		}

		t.Log("Case insensitivity tests completed")
	})
}
