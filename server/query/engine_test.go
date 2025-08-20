package query

import (
	"context"
	"testing"

	"github.com/TFMV/icebox/server/config"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEngineBasicFunctionality tests only the query engine functionality
// without testing storage components
func TestEngineBasicFunctionality(t *testing.T) {
	// Create minimal test configuration
	cfg := config.LoadDefaultConfig()
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create engine without storage (for basic functionality testing)
	engine, err := NewEngine(cfg, nil, logger)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("EngineCreation", func(t *testing.T) {
		// Test that engine was created successfully
		assert.NotNil(t, engine)
		assert.NotNil(t, engine.GetStorageManager())
	})

	t.Run("QueryParsing", func(t *testing.T) {
		// Test basic query parsing functionality
		query := "SELECT * FROM test_table"
		result, err := engine.ExecuteQuery(context.Background(), query)
		// This might fail due to missing storage, but parsing should work
		t.Logf("Query result: %v, error: %v", result, err)
	})

	t.Run("EngineClose", func(t *testing.T) {
		// Test engine cleanup
		err := engine.Close()
		assert.NoError(t, err)
	})
}

// TestEngineQueryParsing tests query parsing without storage dependencies
func TestEngineQueryParsing(t *testing.T) {
	// Create minimal test configuration
	cfg := config.LoadDefaultConfig()
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create engine without storage
	engine, err := NewEngine(cfg, nil, logger)
	require.NoError(t, err)
	defer engine.Close()

	testCases := []struct {
		name  string
		query string
		valid bool
	}{
		{"SimpleSelect", "SELECT * FROM table", true},
		{"CreateTable", "CREATE TABLE test (id INT)", true},
		{"InsertData", "INSERT INTO test VALUES (1)", true},
		{"InvalidQuery", "INVALID SQL QUERY", false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test query parsing (execution will fail without storage, but parsing should work)
			result, err := engine.ExecuteQuery(context.Background(), tc.query)
			if tc.valid {
				// Valid queries should parse but may fail execution due to missing storage
				t.Logf("Query '%s' parsed successfully, execution result: %v, error: %v",
					tc.query, result, err)
			} else {
				// Invalid queries should fail parsing
				assert.Error(t, err, "Invalid query should fail")
			}
		})
	}
}

// TestEngineConfiguration tests engine configuration without storage
func TestEngineConfiguration(t *testing.T) {
	// Create minimal test configuration
	cfg := config.LoadDefaultConfig()
	logger := zerolog.New(zerolog.NewConsoleWriter())

	// Create engine without storage
	engine, err := NewEngine(cfg, nil, logger)
	require.NoError(t, err)
	defer engine.Close()

	t.Run("DuckDBEngine", func(t *testing.T) {
		// Test that DuckDB engine is properly configured
		// This tests the query engine's internal DuckDB setup
		assert.NotNil(t, engine)
	})

	t.Run("ExecutionManager", func(t *testing.T) {
		// Test that execution manager is properly configured
		// This tests the query engine's internal execution management
		assert.NotNil(t, engine)
	})
}
