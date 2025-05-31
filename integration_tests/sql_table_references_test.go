package integration_tests

import (
	"context"
	"strings"
	"testing"

	"github.com/TFMV/icebox/pkg/sdk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSQLTableReferences(t *testing.T) {
	// Create test environment using SDK TestBox
	testBox := sdk.NewTestBox(t, sdk.WithFileSystem()) // Use file system for DuckDB compatibility

	// Create test namespace and table
	namespace := testBox.CreateNamespace("analytics")
	require.NotNil(t, namespace)

	// Create a simple test table
	testTable := testBox.CreateTable("analytics", "users")
	require.NotNil(t, testTable)

	// Register table with engine
	testBox.RegisterTable(testTable)

	t.Run("TableReferenceFormats", func(t *testing.T) {
		testCases := []struct {
			name     string
			query    string
			wantErr  bool
			errCheck func(t *testing.T, err error)
		}{
			{
				name:    "Underscore notation",
				query:   "SELECT * FROM analytics_users",
				wantErr: false,
			},
			{
				name:    "Simple table name",
				query:   "SELECT * FROM users",
				wantErr: false,
			},
			{
				name:    "Dot notation",
				query:   "SELECT * FROM analytics.users",
				wantErr: false, // Now should work after preprocessing
			},
			{
				name:    "Multiple tables with dot notation",
				query:   "SELECT u.id FROM analytics.users u JOIN analytics.products p ON u.id = p.user_id",
				wantErr: true, // Will fail because products table doesn't exist
				errCheck: func(t *testing.T, err error) {
					assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := testBox.ExecuteSQL(tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, result)
				}
			})
		}
	})

	// Test table registration and listing
	t.Run("TableRegistration", func(t *testing.T) {
		// List tables and verify both formats are available
		tables, err := testBox.GetEngine().ListTables(context.Background())
		require.NoError(t, err)
		assert.Contains(t, tables, "analytics_users", "Full table name with underscore should be registered")
		assert.Contains(t, tables, "users", "Simple table name alias should be registered")
	})

	t.Run("DotNotationHandling", func(t *testing.T) {
		testCases := []struct {
			name     string
			query    string
			wantErr  bool
			errCheck func(t *testing.T, err error)
		}{
			{
				name:    "Simple dot notation",
				query:   "SELECT * FROM analytics.users",
				wantErr: false,
			},
			{
				name:    "Underscore notation",
				query:   "SELECT * FROM analytics_users",
				wantErr: false,
			},
			{
				name:    "Simple table name",
				query:   "SELECT * FROM users",
				wantErr: false,
			},
			{
				name:    "Multiple dot notation tables",
				query:   "SELECT u.id, p.name FROM analytics.users u JOIN analytics.products p ON u.id = p.user_id",
				wantErr: true, // Will fail because products table doesn't exist
				errCheck: func(t *testing.T, err error) {
					assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
				},
			},
			{
				name:    "Mixed dot and underscore notation",
				query:   "SELECT * FROM analytics.users JOIN analytics_products ON users.id = products.user_id",
				wantErr: true, // Will fail because products table doesn't exist
				errCheck: func(t *testing.T, err error) {
					assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := testBox.ExecuteSQL(tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, result)
				}
			})
		}
	})

	t.Run("InvalidTableReferences", func(t *testing.T) {
		testCases := []struct {
			name     string
			query    string
			wantErr  bool
			errCheck func(t *testing.T, err error)
		}{
			{
				name:    "Nonexistent table",
				query:   "SELECT * FROM nonexistent_table",
				wantErr: true,
				errCheck: func(t *testing.T, err error) {
					assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
				},
			},
			{
				name:    "Nonexistent schema",
				query:   "SELECT * FROM nonexistent.users",
				wantErr: true,
				errCheck: func(t *testing.T, err error) {
					assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
				},
			},
			{
				name:    "Invalid table name characters",
				query:   "SELECT * FROM 'invalid;table'",
				wantErr: true,
				errCheck: func(t *testing.T, err error) {
					assert.Contains(t, strings.ToLower(err.Error()), "does not exist")
				},
			},
			{
				name:    "SQL injection attempt",
				query:   "SELECT * FROM users; DROP TABLE users;",
				wantErr: true,
				errCheck: func(t *testing.T, err error) {
					assert.Contains(t, strings.ToLower(err.Error()), "sql injection")
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := testBox.ExecuteSQL(tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, result)
				}
			})
		}
	})

	t.Run("TableNameCaseSensitivity", func(t *testing.T) {
		testCases := []struct {
			name     string
			query    string
			wantErr  bool
			errCheck func(t *testing.T, err error)
		}{
			{
				name:    "Lowercase schema and table",
				query:   "SELECT * FROM analytics.users",
				wantErr: false, // DuckDB is case-insensitive by default
			},
			{
				name:    "Uppercase schema",
				query:   "SELECT * FROM ANALYTICS.users",
				wantErr: false, // DuckDB is case-insensitive by default
			},
			{
				name:    "Uppercase table",
				query:   "SELECT * FROM analytics.USERS",
				wantErr: false, // DuckDB is case-insensitive by default
			},
			{
				name:    "Mixed case schema",
				query:   "SELECT * FROM AnAlYtIcS.users",
				wantErr: false, // DuckDB is case-insensitive by default
			},
			{
				name:    "Mixed case table",
				query:   "SELECT * FROM analytics.UsErS",
				wantErr: false, // DuckDB is case-insensitive by default
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := testBox.ExecuteSQL(tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, result)
				}
			})
		}
	})

	t.Run("ColumnReferences", func(t *testing.T) {
		testCases := []struct {
			name     string
			query    string
			wantErr  bool
			errCheck func(t *testing.T, err error)
		}{
			{
				name:    "Simple column reference with underscore notation",
				query:   "SELECT analytics_users.id FROM analytics_users",
				wantErr: false,
			},
			{
				name:    "Table alias with column",
				query:   "SELECT u.id FROM analytics_users u",
				wantErr: false,
			},
			{
				name:    "Full qualified column with underscore",
				query:   "SELECT analytics_users.id FROM analytics_users",
				wantErr: false,
			},
			{
				name:    "Mixed notation with alias",
				query:   "SELECT u.id, u.name FROM analytics_users u",
				wantErr: false,
			},
			{
				name:    "Multiple table references with aliases",
				query:   "SELECT u.id, u.name FROM analytics_users u WHERE u.id > 0",
				wantErr: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				result, err := testBox.ExecuteSQL(tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
					assert.NotNil(t, result)
				}
			})
		}
	})
}
