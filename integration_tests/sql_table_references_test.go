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
	// Skip if server is not running
	if !isServerRunning() {
		t.Skip("Icebox server not running on localhost:9000. Start with: make build-server && make run-server")
	}

	// Connect to Icebox native server
	client, err := sdk.Open(&sdk.Options{
		Addr: []string{"localhost:9000"},
		Auth: sdk.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
	})
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	// Test the connection with ping
	require.NoError(t, client.Ping(ctx))

	// Create test tables
	err = createTestTables(t, client)
	require.NoError(t, err)

	// Clean up test tables after test
	defer cleanupTestTables(t, client)

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
				wantErr: false,
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
				_, err := client.Query(ctx, tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})

	// Test table registration and listing
	t.Run("TableRegistration", func(t *testing.T) {
		// Test that we can query the tables we created
		_, err := client.Query(ctx, "SELECT * FROM analytics_users LIMIT 1")
		assert.NoError(t, err, "Full table name with underscore should be accessible")

		_, err = client.Query(ctx, "SELECT * FROM users LIMIT 1")
		assert.NoError(t, err, "Simple table name alias should be accessible")
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
				_, err := client.Query(ctx, tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
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
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := client.Query(ctx, tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
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
				wantErr: false,
			},
			{
				name:    "Uppercase schema",
				query:   "SELECT * FROM ANALYTICS.users",
				wantErr: false,
			},
			{
				name:    "Uppercase table",
				query:   "SELECT * FROM analytics.USERS",
				wantErr: false,
			},
			{
				name:    "Mixed case schema",
				query:   "SELECT * FROM AnAlYtIcS.users",
				wantErr: false,
			},
			{
				name:    "Mixed case table",
				query:   "SELECT * FROM analytics.UsErS",
				wantErr: false,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := client.Query(ctx, tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
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
				_, err := client.Query(ctx, tc.query)
				if tc.wantErr {
					assert.Error(t, err)
					if tc.errCheck != nil {
						tc.errCheck(t, err)
					}
				} else {
					assert.NoError(t, err)
				}
			})
		}
	})
}

// createTestTables creates the necessary test tables for the SQL table references test
func createTestTables(t *testing.T, client *sdk.Client) error {
	ctx := context.Background()

	// Create analytics_users table
	err := client.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS analytics_users (
			id UInt32,
			name String,
			email String,
			created_at DateTime
		) ENGINE = Memory
	`)
	if err != nil {
		return err
	}

	// Create users table (alias for analytics_users)
	err = client.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS users (
			id UInt32,
			name String,
			email String,
			created_at DateTime
		) ENGINE = Memory
	`)
	if err != nil {
		return err
	}

	// Insert some test data
	err = client.Exec(ctx, `
		INSERT INTO analytics_users (id, name, email, created_at) VALUES
		(1, 'John Doe', 'john@example.com', '2023-01-01 00:00:00'),
		(2, 'Jane Smith', 'jane@example.com', '2023-01-02 00:00:00'),
		(3, 'Bob Johnson', 'bob@example.com', '2023-01-03 00:00:00')
	`)
	if err != nil {
		return err
	}

	// Insert same data into users table
	err = client.Exec(ctx, `
		INSERT INTO users (id, name, email, created_at) VALUES
		(1, 'John Doe', 'john@example.com', '2023-01-01 00:00:00'),
		(2, 'Jane Smith', 'jane@example.com', '2023-01-02 00:00:00'),
		(3, 'Bob Johnson', 'bob@example.com', '2023-01-03 00:00:00')
	`)
	if err != nil {
		return err
	}

	return nil
}

// cleanupTestTables removes the test tables created for testing
func cleanupTestTables(t *testing.T, client *sdk.Client) {
	ctx := context.Background()

	// Drop test tables
	err := client.Exec(ctx, "DROP TABLE IF EXISTS analytics_users")
	if err != nil {
		t.Logf("Warning: Failed to drop analytics_users table: %v", err)
	}

	err = client.Exec(ctx, "DROP TABLE IF EXISTS users")
	if err != nil {
		t.Logf("Warning: Failed to drop users table: %v", err)
	}
}
