package iceberg

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/server/metadata/registry"
	"github.com/TFMV/icebox/server/metadata/registry/regtypes"
	"github.com/TFMV/icebox/server/paths"
	"github.com/rs/zerolog"
)

// Package-specific error codes for metadata generation
var (
	MetadataGenerationFailed = errors.MustNewCode("iceberg.metadata.generation_failed")
	MetadataDirectoryFailed  = errors.MustNewCode("iceberg.metadata.directory_failed")
	MetadataFileFailed       = errors.MustNewCode("iceberg.metadata.file_failed")
)

// MetadataGenerator handles the creation of Iceberg metadata files
type MetadataGenerator struct {
	pathManager paths.PathManager
	logger      zerolog.Logger
	avroCodec   *AvroCodec
}

// NewMetadataGenerator creates a new metadata generator
func NewMetadataGenerator(pathManager paths.PathManager, logger zerolog.Logger) *MetadataGenerator {
	return &MetadataGenerator{
		pathManager: pathManager,
		logger:      logger,
		avroCodec:   NewAvroCodec(),
	}
}

// ManifestEntry represents a file entry in an Iceberg manifest
type ManifestEntry struct {
	Status             int      `json:"status"`               // 0 = existing, 1 = added, 2 = deleted
	SnapshotID         int64    `json:"snapshot_id"`          // ID of the snapshot this entry belongs to
	DataSequenceNumber int64    `json:"data_sequence_number"` // Sequence number for the data file
	FileSequenceNumber int64    `json:"file_sequence_number"` // Sequence number for the manifest file
	DataFile           DataFile `json:"data_file"`            // Information about the data file
}

// DataFile represents a data file in the Iceberg table
type DataFile struct {
	Content         int                    `json:"content"`            // 0 = data, 1 = position deletes, 2 = equality deletes
	FilePath        string                 `json:"file_path"`          // Path to the data file
	FileFormat      string                 `json:"file_format"`        // Format of the file (e.g., "PARQUET")
	Partition       map[string]interface{} `json:"partition"`          // Partition values
	RecordCount     int64                  `json:"record_count"`       // Number of records in the file
	FileSizeInBytes int64                  `json:"file_size_in_bytes"` // Size of the file in bytes
	ColumnSizes     map[int]int64          `json:"column_sizes"`       // Size of each column in bytes
	ValueCounts     map[int]int64          `json:"value_counts"`       // Number of values in each column
	NullValueCounts map[int]int64          `json:"null_value_counts"`  // Number of null values in each column
	LowerBounds     map[int][]byte         `json:"lower_bounds"`       // Lower bounds for each column
	UpperBounds     map[int][]byte         `json:"upper_bounds"`       // Upper bounds for each column
	KeyMetadata     []byte                 `json:"key_metadata"`       // Additional metadata for the file
	SplitOffsets    []int64                `json:"split_offsets"`      // Split offsets for the file
	SortOrderID     int                    `json:"sort_order_id"`      // ID of the sort order used
}

// Snapshot represents an Iceberg table snapshot
type Snapshot struct {
	SnapshotID       int64             `json:"snapshot_id"`
	TimestampMs      int64             `json:"timestamp_ms"`
	Summary          map[string]string `json:"summary"`
	ManifestList     string            `json:"manifest_list"`
	SchemaID         int               `json:"schema_id"`
	ParentSnapshotID *int64            `json:"parent_snapshot_id,omitempty"`
	Manifests        []ManifestFile    `json:"manifests"`
}

// ManifestFile represents a manifest file in the snapshot
type ManifestFile struct {
	ManifestPath      string `json:"manifest_path"`
	ManifestLength    int64  `json:"manifest_length"`
	PartitionSpecID   int    `json:"partition_spec_id"`
	AddedFilesCount   int    `json:"added_files_count"`
	DeletedFilesCount int    `json:"deleted_files_count"`
	AddedRowsCount    int64  `json:"added_rows_count"`
	DeletedRowsCount  int64  `json:"deleted_rows_count"`
}

// GenerateManifest creates an Iceberg manifest file for a batch of files
func (mg *MetadataGenerator) GenerateManifest(ctx context.Context, batch BatchInfo, tableInfo *registry.CompleteTableInfo) (string, error) {
	// Get manifest directory path using actual database and table names
	manifestDir := mg.pathManager.GetTableManifestPath([]string{tableInfo.Database}, tableInfo.Name)
	if err := mg.ensureDirectory(manifestDir); err != nil {
		return "", errors.New(errors.CommonInternal, "while creating manifest directory", err).AddContext("path", manifestDir)
	}

	// Generate manifest filename
	manifestFile := "manifest-" + batch.ID + "-" + strconv.FormatInt(time.Now().Unix(), 10) + ".avro"
	manifestPath := filepath.Join(manifestDir, manifestFile)

	// Create manifest entries for each file
	var entries []ManifestEntry
	for _, file := range batch.Files {
		entry := ManifestEntry{
			Status:             1,                     // 1 = added
			SnapshotID:         time.Now().UnixNano(), // TODO: Get actual snapshot ID
			DataSequenceNumber: 1,                     // TODO: Implement proper sequence numbering
			FileSequenceNumber: 1,                     // TODO: Implement proper sequence numbering
			DataFile: DataFile{
				Content:         0, // 0 = data
				FilePath:        file.FilePath,
				FileFormat:      "PARQUET",
				Partition:       make(map[string]interface{}), // TODO: Parse partition path
				RecordCount:     file.RowCount,
				FileSizeInBytes: file.FileSize,
				ColumnSizes:     make(map[int]int64),  // TODO: Calculate actual column sizes
				ValueCounts:     make(map[int]int64),  // TODO: Calculate actual value counts
				NullValueCounts: make(map[int]int64),  // TODO: Calculate actual null counts
				LowerBounds:     make(map[int][]byte), // TODO: Calculate actual bounds
				UpperBounds:     make(map[int][]byte), // TODO: Calculate actual bounds
				KeyMetadata:     []byte{},             // TODO: Add actual key metadata
				SplitOffsets:    []int64{},            // TODO: Add actual split offsets
				SortOrderID:     0,                    // TODO: Get actual sort order ID
			},
		}
		entries = append(entries, entry)
	}

	// Write manifest file (for now, as JSON - TODO: Implement proper Avro format)
	manifestData := map[string]interface{}{
		"manifest_type":       "ENTRY",
		"content":             0, // 0 = data, 1 = deletes
		"sequence_number":     1,
		"min_sequence_number": 1,
		"entries":             entries,
	}

	manifestBytes, err := json.MarshalIndent(manifestData, "", "  ")
	if err != nil {
		return "", errors.New(errors.CommonInternal, "while marshaling manifest data", err)
	}

	if err := os.WriteFile(manifestPath, manifestBytes, 0644); err != nil {
		return "", errors.New(errors.CommonInternal, "while writing manifest file", err).AddContext("path", manifestPath)
	}

	mg.logger.Debug().
		Str("manifest_path", manifestPath).
		Int("file_count", len(batch.Files)).
		Msg("Generated Iceberg manifest")

	return manifestPath, nil
}

// UpdateMetadataFile updates the Iceberg metadata file with new snapshot information
func (mg *MetadataGenerator) UpdateMetadataFile(ctx context.Context, batch BatchInfo, manifestPath string, tableInfo *registry.CompleteTableInfo) error {
	// Get metadata directory path using actual database and table names
	metadataDir := mg.pathManager.GetTableMetadataPath([]string{tableInfo.Database}, tableInfo.Name)
	if err := mg.ensureDirectory(metadataDir); err != nil {
		return errors.New(errors.CommonInternal, "while creating metadata directory", err).AddContext("path", metadataDir)
	}

	// Generate metadata filename
	metadataFile := "metadata-" + strconv.FormatInt(time.Now().Unix(), 10) + ".avro"
	metadataPath := filepath.Join(metadataDir, metadataFile)

	// Create snapshot
	snapshot := Snapshot{
		SnapshotID:  time.Now().UnixNano(),
		TimestampMs: time.Now().UnixMilli(),
		Summary: map[string]string{
			"operation":        "append",
			"added-files":      strconv.Itoa(len(batch.Files)),
			"added-records":    strconv.FormatInt(mg.calculateTotalRows(batch.Files), 10),
			"added-files-size": strconv.FormatInt(mg.calculateTotalSize(batch.Files), 10),
		},
		ManifestList: "", // TODO: Implement manifest list
		SchemaID:     0,  // TODO: Get actual schema ID
		Manifests: []ManifestFile{
			{
				ManifestPath:      manifestPath,
				ManifestLength:    0, // TODO: Calculate actual file size
				PartitionSpecID:   0, // TODO: Get actual partition spec ID
				AddedFilesCount:   len(batch.Files),
				DeletedFilesCount: 0,
				AddedRowsCount:    mg.calculateTotalRows(batch.Files),
				DeletedRowsCount:  0,
			},
		},
	}

	// Encode snapshot to Avro format
	metadataBytes, err := mg.avroCodec.EncodeSnapshot(&snapshot)
	if err != nil {
		return errors.New(errors.CommonInternal, "while encoding snapshot to Avro", err)
	}

	// Write Avro file with proper header
	if err := mg.avroCodec.WriteAvroFile(metadataPath, metadataBytes, "snapshot"); err != nil {
		return errors.New(errors.CommonInternal, "while writing Avro metadata file", err).AddContext("path", metadataPath)
	}

	mg.logger.Debug().
		Str("metadata_path", metadataPath).
		Str("batch_id", batch.ID).
		Msg("Updated Iceberg metadata file")

	return nil
}

// ensureDirectory creates a directory if it doesn't exist
func (mg *MetadataGenerator) ensureDirectory(path string) error {
	if err := os.MkdirAll(path, 0755); err != nil {
		return err
	}
	return nil
}

// calculateTotalRows calculates the total number of rows in a batch
func (mg *MetadataGenerator) calculateTotalRows(files []*regtypes.TableFile) int64 {
	var total int64
	for _, file := range files {
		total += file.RowCount
	}
	return total
}

// calculateTotalSize calculates the total size of files in a batch
func (mg *MetadataGenerator) calculateTotalSize(files []*regtypes.TableFile) int64 {
	var total int64
	for _, file := range files {
		total += file.FileSize
	}
	return total
}
