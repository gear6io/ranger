package paths

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPathManager(t *testing.T) {
	// Test concrete implementation
	pm := NewManager("/tmp/test")
	require.NotNil(t, pm)

	t.Run("BasePaths", func(t *testing.T) {
		assert.Equal(t, "/tmp/test", pm.GetBasePath())
		assert.Equal(t, "/tmp/test/catalog", pm.GetCatalogPath())
		assert.Equal(t, "/tmp/test/data", pm.GetDataPath())
		assert.Equal(t, "/tmp/test/.ranger", pm.GetInternalMetadataPath())
		assert.Equal(t, "/tmp/test/.ranger/metadata.db", pm.GetInternalMetadataDBPath())
		assert.Equal(t, "/tmp/test/.ranger/migrations", pm.GetMigrationsPath())
	})

	t.Run("TablePaths", func(t *testing.T) {
		assert.Equal(t, "/tmp/test/tables/db1/table1/data", pm.GetTableDataPath([]string{"db1"}, "table1"))
		assert.Equal(t, "/tmp/test/tables/db1/table1/metadata", pm.GetTableMetadataPath([]string{"db1"}, "table1"))
		assert.Equal(t, "/tmp/test/tables/db1/table1/metadata/v1.metadata.json", pm.GetTableMetadataFile("db1", "table1", 1))
	})

	t.Run("CatalogURIs", func(t *testing.T) {
		assert.Equal(t, "/tmp/test/catalog/catalog.json", pm.GetCatalogURI("json"))
		assert.Equal(t, "/tmp/test/catalog/catalog.db", pm.GetCatalogURI("sqlite"))
		assert.Equal(t, "", pm.GetCatalogURI("invalid"))
	})
}

func TestMockPathManager(t *testing.T) {
	// Test mock implementation
	mock := &MockPathManager{BasePath: "/tmp/mock"}
	require.NotNil(t, mock)

	t.Run("BasePaths", func(t *testing.T) {
		assert.Equal(t, "/tmp/mock", mock.GetBasePath())
		assert.Equal(t, "/tmp/mock/catalog", mock.GetCatalogPath())
		assert.Equal(t, "/tmp/mock/data", mock.GetDataPath())
	})

	t.Run("TablePaths", func(t *testing.T) {
		assert.Equal(t, "/tmp/mock/tables/db1/table1/data", mock.GetTableDataPath([]string{"db1"}, "table1"))
		assert.Equal(t, "/tmp/mock/tables/db1/table1/metadata", mock.GetTableMetadataPath([]string{"db1"}, "table1"))
	})
}
