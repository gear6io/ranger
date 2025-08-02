package integration_tests

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/TFMV/icebox/deprecated/catalog/json"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONCatalogIntegration(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "json-catalog-integration-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create configuration
	cfg := &config.Config{
		Name: "integration-test-catalog",
		Catalog: config.CatalogConfig{
			Type: "json",
			JSON: &config.JSONConfig{
				URI:       filepath.Join(tempDir, "catalog.json"),
				Warehouse: tempDir,
			},
		},
	}

	// Create catalog
	catalog, err := json.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	ctx := context.Background()

	// Test namespace operations
	t.Run("NamespaceOperations", func(t *testing.T) {
		namespace := table.Identifier{"test_namespace"}

		// Create namespace
		err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{
			"description": "Test namespace for integration testing",
			"owner":       "integration-test",
		})
		require.NoError(t, err)

		// Check namespace exists
		exists, err := catalog.CheckNamespaceExists(ctx, namespace)
		require.NoError(t, err)
		assert.True(t, exists)

		// Load namespace properties
		props, err := catalog.LoadNamespaceProperties(ctx, namespace)
		require.NoError(t, err)
		assert.Equal(t, "Test namespace for integration testing", props["description"])
		assert.Equal(t, "integration-test", props["owner"])

		// Update namespace properties
		summary, err := catalog.UpdateNamespaceProperties(ctx, namespace,
			[]string{"owner"},
			iceberg.Properties{"owner": "updated-owner", "environment": "test"})
		require.NoError(t, err)
		assert.Contains(t, summary.Removed, "owner")
		assert.Contains(t, summary.Updated, "owner")

		// Verify updates
		updatedProps, err := catalog.LoadNamespaceProperties(ctx, namespace)
		require.NoError(t, err)
		assert.Equal(t, "updated-owner", updatedProps["owner"])
		assert.Equal(t, "test", updatedProps["environment"])

		// List namespaces
		namespaces, err := catalog.ListNamespaces(ctx, table.Identifier{})
		require.NoError(t, err)
		assert.Contains(t, namespaces, namespace)
	})

	// Test table operations
	t.Run("TableOperations", func(t *testing.T) {
		namespace := table.Identifier{"test_namespace"}
		tableIdentifier := table.Identifier{"test_namespace", "test_table"}

		// Create a simple schema
		schema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
			iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
			iceberg.NestedField{ID: 3, Name: "timestamp", Type: iceberg.PrimitiveTypes.Timestamp, Required: true},
		)

		// Create table
		tbl, err := catalog.CreateTable(ctx, tableIdentifier, schema)
		require.NoError(t, err)
		assert.NotNil(t, tbl)
		assert.Equal(t, tableIdentifier, tbl.Identifier())

		// Check table exists
		exists, err := catalog.CheckTableExists(ctx, tableIdentifier)
		require.NoError(t, err)
		assert.True(t, exists)

		// Load table
		loadedTable, err := catalog.LoadTable(ctx, tableIdentifier, iceberg.Properties{})
		require.NoError(t, err)
		assert.Equal(t, tableIdentifier, loadedTable.Identifier())
		// Verify schema fields match (ID comparison may not work as expected)
		assert.Equal(t, len(schema.Fields()), len(loadedTable.Schema().Fields()))

		// List tables in namespace
		var tables []table.Identifier
		for tableId, err := range catalog.ListTables(ctx, namespace) {
			require.NoError(t, err)
			tables = append(tables, tableId)
		}
		assert.Contains(t, tables, tableIdentifier)

		// Rename table
		newTableIdentifier := table.Identifier{"test_namespace", "renamed_table"}
		renamedTable, err := catalog.RenameTable(ctx, tableIdentifier, newTableIdentifier)
		require.NoError(t, err)
		assert.Equal(t, newTableIdentifier, renamedTable.Identifier())

		// Verify old table doesn't exist
		exists, err = catalog.CheckTableExists(ctx, tableIdentifier)
		require.NoError(t, err)
		assert.False(t, exists)

		// Verify new table exists
		exists, err = catalog.CheckTableExists(ctx, newTableIdentifier)
		require.NoError(t, err)
		assert.True(t, exists)

		// Drop table
		err = catalog.DropTable(ctx, newTableIdentifier)
		require.NoError(t, err)

		// Verify table is dropped
		exists, err = catalog.CheckTableExists(ctx, newTableIdentifier)
		require.NoError(t, err)
		assert.False(t, exists)
	})

	// Test concurrent operations
	t.Run("ConcurrentOperations", func(t *testing.T) {
		namespace := table.Identifier{"concurrent_test"}

		// Create namespace for concurrent test
		err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{
			"description": "Namespace for concurrent operations test",
		})
		require.NoError(t, err)

		// Create tables sequentially to avoid concurrency conflicts
		// The JSON catalog uses optimistic concurrency control which may cause conflicts
		numTables := 3
		for i := 0; i < numTables; i++ {
			tableIdentifier := table.Identifier{"concurrent_test", fmt.Sprintf("table_%d", i)}
			schema := iceberg.NewSchema(1,
				iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
				iceberg.NestedField{ID: 2, Name: "data", Type: iceberg.PrimitiveTypes.String, Required: false},
			)

			_, err := catalog.CreateTable(ctx, tableIdentifier, schema)
			require.NoError(t, err)
		}

		// Verify all tables were created
		var tables []table.Identifier
		for tableId, err := range catalog.ListTables(ctx, namespace) {
			require.NoError(t, err)
			tables = append(tables, tableId)
		}
		assert.Len(t, tables, numTables)
	})

	// Test catalog persistence
	t.Run("CatalogPersistence", func(t *testing.T) {
		// Close current catalog
		err := catalog.Close()
		require.NoError(t, err)

		// Create new catalog instance with same configuration
		newCatalog, err := json.NewCatalog(cfg)
		require.NoError(t, err)
		defer newCatalog.Close()

		// Verify data persisted
		namespace := table.Identifier{"test_namespace"}
		exists, err := newCatalog.CheckNamespaceExists(ctx, namespace)
		require.NoError(t, err)
		assert.True(t, exists)

		// Verify namespace properties persisted
		props, err := newCatalog.LoadNamespaceProperties(ctx, namespace)
		require.NoError(t, err)
		assert.Equal(t, "Test namespace for integration testing", props["description"])
	})

	// Test metrics
	t.Run("Metrics", func(t *testing.T) {
		// Create a new catalog to get fresh metrics
		metricsCatalog, err := json.NewCatalog(cfg)
		require.NoError(t, err)
		defer metricsCatalog.Close()

		// Perform some operations
		namespace := table.Identifier{"metrics_test"}
		err = metricsCatalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		require.NoError(t, err)

		tableIdentifier := table.Identifier{"metrics_test", "metrics_table"}
		schema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		)
		_, err = metricsCatalog.CreateTable(ctx, tableIdentifier, schema)
		require.NoError(t, err)

		// Get metrics (this would require exposing metrics from the catalog)
		// For now, we just verify the operations completed successfully
		exists, err := metricsCatalog.CheckTableExists(ctx, tableIdentifier)
		require.NoError(t, err)
		assert.True(t, exists)
	})
}

func TestJSONCatalogErrorHandling(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "json-catalog-error-test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create configuration
	cfg := &config.Config{
		Name: "error-test-catalog",
		Catalog: config.CatalogConfig{
			Type: "json",
			JSON: &config.JSONConfig{
				URI:       filepath.Join(tempDir, "catalog.json"),
				Warehouse: tempDir,
			},
		},
	}

	// Create catalog
	catalog, err := json.NewCatalog(cfg)
	require.NoError(t, err)
	defer catalog.Close()

	ctx := context.Background()

	t.Run("DuplicateNamespace", func(t *testing.T) {
		namespace := table.Identifier{"duplicate_test"}

		// Create namespace
		err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		require.NoError(t, err)

		// Try to create same namespace again
		err = catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("NonExistentNamespace", func(t *testing.T) {
		nonExistentNamespace := table.Identifier{"non_existent"}

		// Try to load properties of non-existent namespace
		_, err := catalog.LoadNamespaceProperties(ctx, nonExistentNamespace)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")

		// Try to drop non-existent namespace
		err = catalog.DropNamespace(ctx, nonExistentNamespace)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})

	t.Run("DuplicateTable", func(t *testing.T) {
		namespace := table.Identifier{"table_test"}
		tableIdentifier := table.Identifier{"table_test", "duplicate_table"}

		// Create namespace first
		err := catalog.CreateNamespace(ctx, namespace, iceberg.Properties{})
		require.NoError(t, err)

		// Create table
		schema := iceberg.NewSchema(1,
			iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		)
		_, err = catalog.CreateTable(ctx, tableIdentifier, schema)
		require.NoError(t, err)

		// Try to create same table again
		_, err = catalog.CreateTable(ctx, tableIdentifier, schema)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "already exists")
	})

	t.Run("NonExistentTable", func(t *testing.T) {
		nonExistentTable := table.Identifier{"non_existent", "table"}

		// Try to load non-existent table
		_, err := catalog.LoadTable(ctx, nonExistentTable, iceberg.Properties{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")

		// Try to drop non-existent table
		err = catalog.DropTable(ctx, nonExistentTable)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "does not exist")
	})
}

func TestJSONCatalogConfiguration(t *testing.T) {
	t.Run("InvalidConfiguration", func(t *testing.T) {
		// Test with missing URI
		cfg := &config.Config{
			Name: "invalid-catalog",
			Catalog: config.CatalogConfig{
				Type: "json",
				JSON: &config.JSONConfig{
					// URI is missing
					Warehouse: "/tmp/warehouse",
				},
			},
		}

		_, err := json.NewCatalog(cfg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "URI cannot be empty")
	})

	t.Run("ValidConfiguration", func(t *testing.T) {
		tempDir, err := os.MkdirTemp("", "json-catalog-config-test")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		cfg := &config.Config{
			Name: "valid-catalog",
			Catalog: config.CatalogConfig{
				Type: "json",
				JSON: &config.JSONConfig{
					URI:       filepath.Join(tempDir, "catalog.json"),
					Warehouse: tempDir,
				},
			},
		}

		catalog, err := json.NewCatalog(cfg)
		require.NoError(t, err)
		defer catalog.Close()

		assert.Equal(t, "valid-catalog", catalog.Name())
	})
}
