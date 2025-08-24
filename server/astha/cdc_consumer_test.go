package astha

import (
	"testing"

	"github.com/gear6io/ranger/server/metadata/registry"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/stretchr/testify/assert"
)

func TestGenericEventConversion(t *testing.T) {
	// Create a sample CDC change for table_files
	// Note: We'll use simple fields that don't have time.Time to avoid parsing issues
	change := registry.CDCLogEntry{
		ID:        1,
		Timestamp: "2024-01-01 12:00:00.000000000",
		CreatedAt: "2024-01-01 12:00:00.000000000",
		TableName: "table_files",
		Operation: "INSERT",
		After:     `{"id": 1, "table_id": 100, "file_name": "file1.parquet", "file_path": "/data/file1.parquet", "file_size": 1024, "file_type": "parquet", "row_count": 1000, "is_compressed": false, "iceberg_metadata_state": "pending"}`,
		Before:    "",
	}

	t.Run("ConvertToTableFileEvent maintains type safety", func(t *testing.T) {
		event, err := ConvertToTableFileEvent(change)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), event.ID)
		assert.Equal(t, "table_files", event.Table)
		assert.Equal(t, "INSERT", event.Operation)

		// This is the key: event.Data is regtypes.TableFile, not any!
		tableFile := event.Data
		assert.Equal(t, int64(1), tableFile.ID)
		assert.Equal(t, int64(100), tableFile.TableID)
		assert.Equal(t, "file1.parquet", tableFile.FileName)
		assert.Equal(t, "/data/file1.parquet", tableFile.FilePath)
		assert.Equal(t, int64(1024), tableFile.FileSize)
		assert.Equal(t, "parquet", tableFile.FileType)
		assert.Equal(t, int64(1000), tableFile.RowCount)
		assert.Equal(t, false, tableFile.IsCompressed)
		assert.Equal(t, "pending", tableFile.IcebergMetadataState)

		// Type safety: we can access TableFile-specific fields
		// This would not compile if event.Data was 'any'
		_ = tableFile.FilePath // This is a string, not interface{}
		_ = tableFile.FileSize // This is int64, not interface{}
		_ = tableFile.FileName // This is a string, not interface{}
	})

	t.Run("ConvertToTableEvent maintains type safety", func(t *testing.T) {
		// Create a sample CDC change for tables
		// Note: We'll use simple fields that don't have time.Time to avoid parsing issues
		tableChange := registry.CDCLogEntry{
			ID:        2,
			Timestamp: "2024-01-01 12:00:00.000000000",
			CreatedAt: "2024-01-01 12:00:00.000000000",
			TableName: "tables",
			Operation: "INSERT",
			After:     `{"id": 1, "database_id": 1, "name": "test_table", "display_name": "Test Table", "description": "A test table", "table_type": "user", "is_temporary": false, "is_external": false, "row_count": 0, "file_count": 0, "total_size": 0}`,
			Before:    "",
		}

		event, err := ConvertToTableEvent(tableChange)
		assert.NoError(t, err)
		assert.Equal(t, int64(2), event.ID)
		assert.Equal(t, "tables", event.Table)

		// Type safety: event.Data is regtypes.Table, not any!
		table := event.Data
		assert.Equal(t, int64(1), table.ID)
		assert.Equal(t, int64(1), table.DatabaseID)
		assert.Equal(t, "test_table", table.Name)
		assert.Equal(t, "Test Table", table.DisplayName)
		assert.Equal(t, "A test table", table.Description)
		assert.Equal(t, "user", table.TableType)
		assert.Equal(t, false, table.IsTemporary)
		assert.Equal(t, false, table.IsExternal)
		assert.Equal(t, int64(0), table.RowCount)
		assert.Equal(t, 0, table.FileCount)
		assert.Equal(t, int64(0), table.TotalSize)

		// We can access Table-specific fields with full type safety
		_ = table.Name        // This is a string
		_ = table.DisplayName // This is a string
		_ = table.DatabaseID  // This is int64
	})

	t.Run("Generic function handles different types correctly", func(t *testing.T) {
		// Test the standalone generic function directly
		event, err := convertChangeToEventGeneric[regtypes.TableFile](change)
		assert.NoError(t, err)
		assert.IsType(t, regtypes.TableFile{}, event.Data)

		// Test with a different type
		event2, err := convertChangeToEventGeneric[regtypes.Table](change)
		assert.NoError(t, err)
		assert.IsType(t, regtypes.Table{}, event2.Data)
	})
}

func TestParseDataToType(t *testing.T) {
	change := registry.CDCLogEntry{
		Operation: "INSERT",
		After:     `{"id": 1, "database_id": 1, "name": "test", "display_name": "Test", "description": "Test table", "table_type": "user", "is_temporary": false, "is_external": false, "row_count": 0, "file_count": 0, "total_size": 0}`,
	}

	t.Run("parseDataToType maintains type safety", func(t *testing.T) {
		var result regtypes.Table
		err := parseDataToType[regtypes.Table](change, &result)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), result.ID)
		assert.Equal(t, "test", result.Name)

		// Type safety: result is regtypes.Table, not any
		_ = result.Name // This is a string
		_ = result.ID   // This is int64
	})
}
