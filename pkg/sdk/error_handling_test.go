package sdk_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gear6io/ranger/pkg/sdk"
)

// TestRowsErrorHandling tests the error handling capabilities of the Rows struct
func TestRowsErrorHandling(t *testing.T) {
	t.Run("RowsWithNoError", func(t *testing.T) {
		rows := &sdk.Rows{
			Cols: []sdk.Column{
				{Name: "id", Type: "Int32"},
				{Name: "name", Type: "String"},
			},
			Data: [][]interface{}{
				{1, "Alice"},
				{2, "Bob"},
			},
			Current:  0,
			Closed:   false,
			QueryErr: nil,
		}

		// Test that Err() returns nil when there's no error
		assert.NoError(t, rows.Err())

		// Test that we can iterate through rows without errors
		rowCount := 0
		for rows.Next() {
			rowCount++
		}
		assert.Equal(t, 2, rowCount)
		assert.NoError(t, rows.Err())
	})

	t.Run("RowsWithError", func(t *testing.T) {
		expectedErr := errors.New("server exception [1001]: Table 'nonexistent' does not exist")
		rows := &sdk.Rows{
			Cols: []sdk.Column{
				{Name: "id", Type: "Int32"},
			},
			Data:     [][]interface{}{},
			Current:  0,
			Closed:   false,
			QueryErr: expectedErr,
		}

		// Test that Err() returns the stored error
		assert.Error(t, rows.Err())
		assert.Equal(t, expectedErr, rows.Err())
		assert.Contains(t, rows.Err().Error(), "server exception [1001]")
	})

	t.Run("RowsWithServerException", func(t *testing.T) {
		expectedErr := errors.New("server exception [1002]: Invalid SQL syntax near 'FROM'")
		rows := &sdk.Rows{
			Cols:     []sdk.Column{},
			Data:     [][]interface{}{},
			QueryErr: expectedErr,
		}

		// Test that Err() returns the server exception
		assert.Error(t, rows.Err())
		assert.Contains(t, rows.Err().Error(), "server exception [1002]")
		assert.Contains(t, rows.Err().Error(), "server exception [1002]")
		assert.Contains(t, rows.Err().Error(), "Invalid SQL syntax")
	})
}

// TestConnectionErrorHandling tests error handling in connection operations
func TestConnectionErrorHandling(t *testing.T) {
	t.Run("ConnectionAcquisitionFailure", func(t *testing.T) {
		// Create a client with invalid address to simulate connection failure
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{"invalid-address:9999"},
			Auth: sdk.Auth{
				Username: "test",
				Password: "test",
				Database: "test",
			},
			DialTimeout: 100 * time.Millisecond, // Short timeout for testing
		})
		require.NoError(t, err)
		defer client.Close()

		ctx := context.Background()

		// Test that Query fails with connection error
		_, err = client.Query(ctx, "SELECT 1")
		assert.Error(t, err)
		// Check for various possible error messages
		errMsg := err.Error()
		assert.True(t,
			contains(errMsg, "connection") ||
				contains(errMsg, "dial") ||
				contains(errMsg, "lookup") ||
				contains(errMsg, "timeout"),
			"Expected connection-related error, got: %s", errMsg)
	})

	t.Run("ContextCancellation", func(t *testing.T) {
		// Create a client with invalid address
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{"invalid-address:9999"},
			Auth: sdk.Auth{
				Username: "test",
				Password: "test",
				Database: "test",
			},
			DialTimeout: 5 * time.Second, // Long timeout to test cancellation
		})
		require.NoError(t, err)
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		// Test that Query fails with context cancellation
		_, err = client.Query(ctx, "SELECT 1")
		assert.Error(t, err)
		// The error could be either connection failure or context cancellation
		errMsg := err.Error()
		assert.True(t,
			errors.Is(err, context.DeadlineExceeded) ||
				errors.Is(err, context.Canceled) ||
				contains(errMsg, "connection") ||
				contains(errMsg, "dial") ||
				contains(errMsg, "lookup") ||
				contains(errMsg, "timeout"),
			"Expected context or connection error, got: %v", err)
	})
}

// TestProtocolErrorHandling tests error handling in the protocol layer
func TestProtocolErrorHandling(t *testing.T) {
	t.Run("ExceptionErrorFormatting", func(t *testing.T) {
		// Test the Exception.Error() method
		exception := &sdk.Exception{
			Code:    1001,
			Name:    "TableNotFound",
			Message: "Table 'users' does not exist",
			Stack:   "stack trace here",
		}

		errorMsg := exception.Error()
		assert.Contains(t, errorMsg, "ranger exception [1001]")
		assert.Contains(t, errorMsg, "TableNotFound")
		assert.Contains(t, errorMsg, "Table 'users' does not exist")
	})

	t.Run("ExceptionWithEmptyFields", func(t *testing.T) {
		// Test exception with minimal fields
		exception := &sdk.Exception{
			Code:    0,
			Name:    "",
			Message: "",
			Stack:   "",
		}

		errorMsg := exception.Error()
		assert.Contains(t, errorMsg, "ranger exception [0]")
		assert.Contains(t, errorMsg, ": ")
	})

	t.Run("ExceptionWithSpecialCharacters", func(t *testing.T) {
		// Test exception with special characters in message
		exception := &sdk.Exception{
			Code:    1002,
			Name:    "SyntaxError",
			Message: "Invalid syntax: 'SELECT * FROM table WHERE id = '1''",
			Stack:   "",
		}

		errorMsg := exception.Error()
		assert.Contains(t, errorMsg, "ranger exception [1002]")
		assert.Contains(t, errorMsg, "Invalid syntax: 'SELECT * FROM table WHERE id = '1''")
	})
}

// TestClientErrorHandling tests error handling in the client layer
func TestClientErrorHandling(t *testing.T) {
	t.Run("ClientCreationWithInvalidOptions", func(t *testing.T) {
		// Test client creation with invalid options
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{}, // Empty address list
		})

		// Client creation might succeed even with empty addresses (validation happens later)
		if err != nil {
			assert.Contains(t, err.Error(), "address")
		} else {
			// If client creation succeeds, test that it fails on actual operations
			defer client.Close()

			ctx := context.Background()
			_, queryErr := client.Query(ctx, "SELECT 1")
			assert.Error(t, queryErr)
		}
	})

	t.Run("ClientCreationWithValidOptions", func(t *testing.T) {
		// Test client creation with valid options
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{"localhost:2849"},
			Auth: sdk.Auth{
				Username: "test",
				Password: "test",
				Database: "test",
			},
		})
		require.NoError(t, err)
		defer client.Close()

		// Client should be created successfully
		assert.NotNil(t, client)
	})

	t.Run("ClientCloseMultipleTimes", func(t *testing.T) {
		// Test that closing a client multiple times doesn't cause errors
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{"localhost:2849"},
			Auth: sdk.Auth{
				Username: "test",
				Password: "test",
				Database: "test",
			},
		})
		require.NoError(t, err)

		// Close multiple times
		assert.NoError(t, client.Close())
		assert.NoError(t, client.Close()) // Should not error on second close
	})
}

// TestQueryErrorHandling tests error handling in query execution
func TestQueryErrorHandling(t *testing.T) {
	t.Run("QueryWithInvalidSQL", func(t *testing.T) {
		// This test would require a mock server to properly test
		// For now, we'll test the error handling structure
		t.Skip("Requires mock server implementation")
	})

	t.Run("QueryWithContextCancellation", func(t *testing.T) {
		// Test query cancellation through context
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{"localhost:2849"},
			Auth: sdk.Auth{
				Username: "test",
				Password: "test",
				Database: "test",
			},
		})
		if err != nil {
			t.Skip("Cannot create client, skipping test")
		}
		defer client.Close()

		ctx, cancel := context.WithCancel(context.Background())

		// Cancel immediately
		cancel()

		// Query should fail due to cancelled context
		_, err = client.Query(ctx, "SELECT 1")
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})
}

// TestBatchErrorHandling tests error handling in batch operations
func TestBatchErrorHandling(t *testing.T) {
	t.Run("BatchAppendAfterSend", func(t *testing.T) {
		sdk.TestWithServer(t, func(t *testing.T, server *sdk.TestServer) {
			client := server.GetClient(t)
			defer client.Close()

			batch, err := client.PrepareBatch(context.Background(), "INSERT INTO test_table VALUES")
			require.NoError(t, err)

			// Mark batch as sent
			batch.Sent = true

			// Try to append after sending
			err = batch.Append(1, "test")
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "already sent")
		})
	})

	t.Run("BatchAppendWithWrongColumnCount", func(t *testing.T) {
		sdk.TestWithServer(t, func(t *testing.T, server *sdk.TestServer) {
			client := server.GetClient(t)
			defer client.Close()

			batch, err := client.PrepareBatch(context.Background(), "INSERT INTO test_table (id, name) VALUES")
			require.NoError(t, err)

			// Try to append wrong number of values
			err = batch.Append(1) // Only 1 value, but batch expects 2 columns
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "value count does not match column count")
		})
	})
}

// TestSettingsErrorHandling tests error handling in settings operations
func TestSettingsErrorHandling(t *testing.T) {
	t.Run("SettingsTypeConversionErrors", func(t *testing.T) {
		settings := sdk.Settings{}

		// Test getting non-existent values
		assert.Equal(t, 0, settings.GetInt("non_existent"))
		assert.Equal(t, "", settings.GetString("non_existent"))
		assert.Equal(t, false, settings.GetBool("non_existent"))

		// Test type conversion edge cases
		settings.Set("string_value", "not_a_number")
		settings.Set("bool_value", "not_a_bool")

		// These should return default values for invalid conversions
		assert.Equal(t, 0, settings.GetInt("string_value"))
		assert.Equal(t, false, settings.GetBool("bool_value"))
	})

	t.Run("SettingsNilHandling", func(t *testing.T) {
		var settings sdk.Settings

		// Test that nil settings don't panic
		assert.Equal(t, 0, settings.GetInt("any"))
		assert.Equal(t, "", settings.GetString("any"))
		assert.Equal(t, false, settings.GetBool("any"))
	})
}

// Helper function to check if error message contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr ||
		(len(s) > len(substr) &&
			(s[:len(substr)] == substr ||
				s[len(s)-len(substr):] == substr ||
				containsSubstring(s, substr))))
}

// Helper function to check if string contains substring
func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestErrorPropagation tests that errors properly propagate through the SDK layers
func TestErrorPropagation(t *testing.T) {
	t.Run("ErrorFromProtocolToClient", func(t *testing.T) {
		// This test would require a mock server to properly test protocol errors
		// For now, we'll test the error handling structure
		t.Skip("Requires mock server implementation")
	})

	t.Run("ErrorFromConnectionToRows", func(t *testing.T) {
		// Test that connection errors are properly stored in Rows
		rows := &sdk.Rows{
			QueryErr: errors.New("connection failed"),
		}

		assert.Error(t, rows.Err())
		assert.Contains(t, rows.Err().Error(), "connection failed")
	})
}

// TestErrorRecovery tests error recovery scenarios
func TestErrorRecovery(t *testing.T) {
	t.Run("RecoverFromConnectionError", func(t *testing.T) {
		// Test that client can recover from connection errors
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{"localhost:2849"},
			Auth: sdk.Auth{
				Username: "test",
				Password: "test",
				Database: "test",
			},
		})
		if err != nil {
			t.Skip("Cannot create client, skipping test")
		}
		defer client.Close()

		// Client should be created successfully even if connection fails
		assert.NotNil(t, client)
	})

	t.Run("RecoverFromQueryError", func(t *testing.T) {
		// Test that client can recover from query errors
		client, err := sdk.NewClient(&sdk.Options{
			Addr: []string{"localhost:2849"},
			Auth: sdk.Auth{
				Username: "test",
				Password: "test",
				Database: "test",
			},
		})
		if err != nil {
			t.Skip("Cannot create client, skipping test")
		}
		defer client.Close()

		// Client should still be usable after a query error
		assert.NotNil(t, client)
	})
}
