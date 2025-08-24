package sdk_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/gear6io/ranger/pkg/sdk"
)

func TestSDKBasics(t *testing.T) {
	// Test that we can create options
	logger := zap.NewNop()
	options := &sdk.Options{
		Logger: logger,
		Addr:   []string{"127.0.0.1:2849"},
		Auth: sdk.Auth{
			Username: "default",
			Password: "",
			Database: "default",
		},
	}

	// Test setting defaults
	options = options.SetDefaults()
	assert.Equal(t, []string{"127.0.0.1:2849"}, options.Addr)
	assert.Equal(t, "default", options.Auth.Database)
	assert.Equal(t, "default", options.Auth.Username)
	assert.Equal(t, 3*time.Second, options.ReadTimeout)
	assert.Equal(t, 30*time.Second, options.DialTimeout)
	assert.Equal(t, sdk.Native, options.Protocol)
}

func TestSettings(t *testing.T) {
	// Test settings map operations
	settings := sdk.Settings{}

	// Test setting values
	settings.Set("max_execution_time", 30)
	settings.Set("timezone", "UTC")
	settings.Set("debug", true)

	// Test getting values
	assert.Equal(t, 30, settings.GetInt("max_execution_time"))
	assert.Equal(t, "UTC", settings.GetString("timezone"))
	assert.Equal(t, true, settings.GetBool("debug"))

	// Test getting non-existent values
	assert.Equal(t, 0, settings.GetInt("non_existent"))
	assert.Equal(t, "", settings.GetString("non_existent"))
	assert.Equal(t, false, settings.GetBool("non_existent"))
}

func TestQueryIDGeneration(t *testing.T) {
	// Test that query IDs are generated and unique
	id1 := sdk.GenerateQueryID()
	id2 := sdk.GenerateQueryID()

	assert.NotEmpty(t, id1)
	assert.NotEmpty(t, id2)
	assert.NotEqual(t, id1, id2)
}

func TestParseInsertQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		expected string
		columns  []string
		hasError bool
	}{
		{
			name:     "simple insert",
			query:    "INSERT INTO test_table VALUES (1, 'test')",
			expected: "test_table",
			columns:  nil,
			hasError: false,
		},
		{
			name:     "insert with columns",
			query:    "INSERT INTO test_table (id, name, value) VALUES (1, 'test', 1.23)",
			expected: "test_table",
			columns:  []string{"id", "name", "value"},
			hasError: false,
		},
		{
			name:     "invalid query",
			query:    "SELECT * FROM test_table",
			expected: "",
			columns:  nil,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tableName, columns, err := sdk.ParseInsertQuery(tt.query)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, tableName)
				assert.Equal(t, tt.columns, columns)
			}
		})
	}
}

func TestExceptionError(t *testing.T) {
	exception := &sdk.Exception{
		Code:    1001,
		Name:    "TestException",
		Message: "Test error message",
		Stack:   "test stack trace",
	}

	errorMsg := exception.Error()
	assert.Contains(t, errorMsg, "1001")
	assert.Contains(t, errorMsg, "TestException")
	assert.Contains(t, errorMsg, "Test error message")
}

func TestBatchOperations(t *testing.T) {
	// Create a mock batch
	batch := &sdk.Batch{
		TableName: "test_table",
		Columns:   []string{"id", "name", "value"},
		Data:      make([][]interface{}, 0),
		Sent:      false,
	}

	// Test appending rows
	err := batch.Append(1, "test1", 1.23)
	require.NoError(t, err)
	assert.Equal(t, 1, batch.Rows())

	err = batch.Append(2, "test2", 4.56)
	require.NoError(t, err)
	assert.Equal(t, 2, batch.Rows())

	// Test column operations
	col := batch.Column(0)
	require.NotNil(t, col)
	assert.Equal(t, 0, col.Column)

	// Test that batch is not sent initially
	assert.False(t, batch.IsSent())
}

func TestRowsOperations(t *testing.T) {
	// Create mock rows
	rows := &sdk.Rows{
		Cols: []sdk.Column{
			{Name: "id", Type: "Int32"},
			{Name: "name", Type: "String"},
			{Name: "value", Type: "Float64"},
		},
		Data: [][]interface{}{
			{"1", "test1", "1.23"},
			{"2", "test2", "4.56"},
		},
		Current: 0,
		Closed:  false,
	}

	// Test columns
	columnNames, err := rows.Columns()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "value"}, columnNames)

	// Test iteration
	assert.True(t, rows.Next())
	var id int
	var name string
	var value float64
	err = rows.Scan(&id, &name, &value)
	require.NoError(t, err)
	assert.Equal(t, 1, id)
	assert.Equal(t, "test1", name)
	assert.Equal(t, 1.23, value)

	assert.True(t, rows.Next())
	err = rows.Scan(&id, &name, &value)
	require.NoError(t, err)
	assert.Equal(t, 2, id)
	assert.Equal(t, "test2", name)
	assert.Equal(t, 4.56, value)

	assert.False(t, rows.Next())

	// Test close
	err = rows.Close()
	require.NoError(t, err)
}

func TestContextOperations(t *testing.T) {
	// Test context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// This should not block
	select {
	case <-ctx.Done():
		t.Fatal("Context should not be done yet")
	default:
		// Expected
	}

	// Wait for timeout
	time.Sleep(150 * time.Millisecond)

	select {
	case <-ctx.Done():
		// Expected
	default:
		t.Fatal("Context should be done by now")
	}
}
