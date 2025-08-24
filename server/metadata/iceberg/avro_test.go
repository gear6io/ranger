package iceberg

import (
	"testing"
	"time"

	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/stretchr/testify/assert"
)

func TestAvroCodec_CreateManifestFile(t *testing.T) {
	codec := NewAvroCodec()

	// Create test batch
	batch := BatchInfo{
		ID:        "test-batch-1",
		Files:     []*regtypes.TableFile{},
		CreatedAt: time.Now(),
		Status:    "pending",
	}

	// Add test files
	file1 := &regtypes.TableFile{
		ID:            1,
		TableID:       1,
		FileName:      "test1.parquet",
		FilePath:      "/data/test1.parquet",
		FileSize:      1024,
		FileType:      "PARQUET",
		RowCount:      100,
		PartitionPath: "year=2024/month=1",
	}

	file2 := &regtypes.TableFile{
		ID:            2,
		TableID:       1,
		FileName:      "test2.parquet",
		FilePath:      "/data/test2.parquet",
		FileSize:      2048,
		FileType:      "PARQUET",
		RowCount:      200,
		PartitionPath: "year=2024/month=2",
	}

	batch.Files = append(batch.Files, file1, file2)

	// Create manifest file
	snapshotID := time.Now().UnixNano()
	manifest := codec.CreateManifestFile(batch, snapshotID)

	// Verify manifest structure
	assert.NotNil(t, manifest)
	assert.Equal(t, "", manifest.ManifestPath)         // Will be set later
	assert.Equal(t, int64(0), manifest.ManifestLength) // Will be calculated
	assert.Equal(t, 0, manifest.PartitionSpecID)       // TODO: Get actual
	assert.Equal(t, 2, manifest.AddedFilesCount)
	assert.Equal(t, 0, manifest.DeletedFilesCount)
	assert.Equal(t, int64(0), manifest.AddedRowsCount) // TODO: Calculate
	assert.Equal(t, int64(0), manifest.DeletedRowsCount)
}

func TestAvroCodec_CreateSnapshot(t *testing.T) {
	codec := NewAvroCodec()

	// Create test batch
	batch := BatchInfo{
		ID:        "test-batch-1",
		Files:     []*regtypes.TableFile{},
		CreatedAt: time.Now(),
		Status:    "pending",
	}

	// Add test files
	file1 := &regtypes.TableFile{
		ID:       1,
		TableID:  1,
		FileName: "test1.parquet",
		FilePath: "/data/test1.parquet",
		FileSize: 1024,
		FileType: "PARQUET",
		RowCount: 100,
	}

	file2 := &regtypes.TableFile{
		ID:       2,
		TableID:  1,
		FileName: "test2.parquet",
		FilePath: "/data/test2.parquet",
		FileSize: 2048,
		FileType: "PARQUET",
		RowCount: 200,
	}

	batch.Files = append(batch.Files, file1, file2)

	// Create snapshot
	manifestPath := "/manifests/test.avro"
	snapshot := codec.CreateSnapshot(batch, manifestPath, nil)

	// Verify snapshot structure
	assert.NotNil(t, snapshot)
	assert.Greater(t, snapshot.SnapshotID, int64(0))
	assert.Greater(t, snapshot.TimestampMs, int64(0))
	assert.Equal(t, "append", snapshot.Summary["operation"])
	assert.Equal(t, "2", snapshot.Summary["added-files"])
	assert.Equal(t, "300", snapshot.Summary["added-records"])     // 100 + 200
	assert.Equal(t, "3072", snapshot.Summary["added-files-size"]) // 1024 + 2048
	assert.Equal(t, "", snapshot.ManifestList)                    // TODO: Implement
	assert.Equal(t, 0, snapshot.SchemaID)                         // TODO: Get actual
	assert.Len(t, snapshot.Manifests, 1)

	// Verify manifest info
	manifestInfo := snapshot.Manifests[0]
	assert.Equal(t, manifestPath, manifestInfo.ManifestPath)
	assert.Equal(t, int64(0), manifestInfo.ManifestLength) // TODO: Calculate
	assert.Equal(t, 0, manifestInfo.PartitionSpecID)       // TODO: Get actual
	assert.Equal(t, 2, manifestInfo.AddedFilesCount)
	assert.Equal(t, 0, manifestInfo.DeletedFilesCount)
	assert.Equal(t, int64(300), manifestInfo.AddedRowsCount) // 100 + 200
	assert.Equal(t, int64(0), manifestInfo.DeletedRowsCount)
}

func TestAvroCodec_ParsePartitionPath(t *testing.T) {
	codec := NewAvroCodec()

	// Test empty partition path
	emptyPartition := codec.parsePartitionPath("")
	assert.Empty(t, emptyPartition)

	// Test simple partition path
	simplePartition := codec.parsePartitionPath("year=2024")
	expectedSimple := map[string]interface{}{
		"year": "2024",
	}
	assert.Equal(t, expectedSimple, simplePartition)

	// Test complex partition path
	complexPartition := codec.parsePartitionPath("year=2024/month=1/day=15")
	expectedComplex := map[string]interface{}{
		"year":  "2024",
		"month": "1",
		"day":   "15",
	}
	assert.Equal(t, expectedComplex, complexPartition)

	// Test malformed partition path
	malformedPartition := codec.parsePartitionPath("invalid/format")
	assert.Empty(t, malformedPartition)
}

func TestAvroCodec_ConvertTableFileToManifestEntry(t *testing.T) {
	codec := NewAvroCodec()

	// Create test table file
	tableFile := &regtypes.TableFile{
		ID:            1,
		TableID:       1,
		FileName:      "test.parquet",
		FilePath:      "/data/test.parquet",
		FileSize:      1024,
		FileType:      "PARQUET",
		RowCount:      100,
		PartitionPath: "year=2024/month=1",
	}

	snapshotID := time.Now().UnixNano()
	entry := codec.ConvertTableFileToManifestEntry(tableFile, snapshotID)

	// Verify entry structure
	assert.NotNil(t, entry)
	assert.Equal(t, 1, entry.Status) // 1 = added
	assert.Equal(t, snapshotID, entry.SnapshotID)
	assert.Equal(t, int64(1), entry.DataSequenceNumber) // TODO: Implement proper
	assert.Equal(t, int64(1), entry.FileSequenceNumber) // TODO: Implement proper

	// Verify data file
	dataFile := entry.DataFile
	assert.Equal(t, 0, dataFile.Content) // 0 = data
	assert.Equal(t, "/data/test.parquet", dataFile.FilePath)
	assert.Equal(t, "PARQUET", dataFile.FileFormat)
	assert.Equal(t, int64(100), dataFile.RecordCount)
	assert.Equal(t, int64(1024), dataFile.FileSizeInBytes)

	// Verify partition values
	expectedPartition := map[string]interface{}{
		"year":  "2024",
		"month": "1",
	}
	assert.Equal(t, expectedPartition, dataFile.Partition)
}

func TestSchemaRegistry_GetSchema(t *testing.T) {
	schemaRegistry := NewSchemaRegistry()

	// Test valid schema names
	manifestEntrySchema := schemaRegistry.GetSchema("manifest_entry")
	assert.NotEmpty(t, manifestEntrySchema)
	assert.Contains(t, manifestEntrySchema, "manifest_entry")

	manifestFileSchema := schemaRegistry.GetSchema("manifest_file")
	assert.NotEmpty(t, manifestFileSchema)
	assert.Contains(t, manifestFileSchema, "manifest_file")

	snapshotSchema := schemaRegistry.GetSchema("snapshot")
	assert.NotEmpty(t, snapshotSchema)
	assert.Contains(t, snapshotSchema, "snapshot")

	// Test invalid schema name
	invalidSchema := schemaRegistry.GetSchema("invalid")
	assert.Empty(t, invalidSchema)
}
