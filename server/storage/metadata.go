package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// TableMetadata represents metadata for a table
type TableMetadata struct {
	Name         string     `json:"name"`
	Schema       []byte     `json:"schema"`
	FileCount    int        `json:"file_count"`
	TotalSize    int64      `json:"total_size"`
	LastModified time.Time  `json:"last_modified"`
	Created      time.Time  `json:"created"`
	Files        []FileInfo `json:"files"`
}

// FileInfo represents information about a data file
type FileInfo struct {
	Name     string    `json:"name"`
	Size     int64     `json:"size"`
	Created  time.Time `json:"created"`
	Modified time.Time `json:"modified"`
	Date     time.Time `json:"date"`
}

// MetadataManager handles table metadata operations
type MetadataManager struct {
	BasePath string
}

// NewMetadataManager creates a new metadata manager
func NewMetadataManager(basePath string) *MetadataManager {
	return &MetadataManager{
		BasePath: basePath,
	}
}

// CreateTableMetadata creates metadata for a new table
func (mm *MetadataManager) CreateTableMetadata(tableName string, schema []byte) (*TableMetadata, error) {
	now := time.Now()

	metadata := &TableMetadata{
		Name:         tableName,
		Schema:       schema,
		FileCount:    0,
		TotalSize:    0,
		LastModified: now,
		Created:      now,
		Files:        []FileInfo{},
	}

	// Ensure table directory exists
	tablePath := getTablePath(mm.BasePath, tableName)
	if err := os.MkdirAll(tablePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create table directory: %w", err)
	}

	// Save metadata
	if err := mm.SaveTableMetadata(tableName, metadata); err != nil {
		return nil, fmt.Errorf("failed to save table metadata: %w", err)
	}

	return metadata, nil
}

// LoadTableMetadata loads metadata for a table
func (mm *MetadataManager) LoadTableMetadata(tableName string) (*TableMetadata, error) {
	metadataPath := getMetadataFilePath(mm.BasePath, tableName)

	data, err := os.ReadFile(metadataPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("table metadata not found: %s", tableName)
		}
		return nil, fmt.Errorf("failed to read metadata: %w", err)
	}

	var metadata TableMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse metadata: %w", err)
	}

	return &metadata, nil
}

// SaveTableMetadata saves metadata for a table
func (mm *MetadataManager) SaveTableMetadata(tableName string, metadata *TableMetadata) error {
	metadataPath := getMetadataFilePath(mm.BasePath, tableName)

	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	if err := os.WriteFile(metadataPath, data, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return nil
}

// AddFileToMetadata adds a file to table metadata
func (mm *MetadataManager) AddFileToMetadata(tableName string, fileInfo FileInfo) error {
	metadata, err := mm.LoadTableMetadata(tableName)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	// Check if file already exists
	for i, existingFile := range metadata.Files {
		if existingFile.Name == fileInfo.Name {
			// Update existing file info
			metadata.Files[i] = fileInfo
			metadata.TotalSize = metadata.TotalSize - existingFile.Size + fileInfo.Size
			metadata.LastModified = time.Now()
			return mm.SaveTableMetadata(tableName, metadata)
		}
	}

	// Add new file
	metadata.Files = append(metadata.Files, fileInfo)
	metadata.FileCount = len(metadata.Files)
	metadata.TotalSize += fileInfo.Size
	metadata.LastModified = time.Now()

	return mm.SaveTableMetadata(tableName, metadata)
}

// RemoveFileFromMetadata removes a file from table metadata
func (mm *MetadataManager) RemoveFileFromMetadata(tableName string, fileName string) error {
	metadata, err := mm.LoadTableMetadata(tableName)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	for i, file := range metadata.Files {
		if file.Name == fileName {
			// Remove file from slice
			metadata.Files = append(metadata.Files[:i], metadata.Files[i+1:]...)
			metadata.FileCount = len(metadata.Files)
			metadata.TotalSize -= file.Size
			metadata.LastModified = time.Now()

			return mm.SaveTableMetadata(tableName, metadata)
		}
	}

	return fmt.Errorf("file not found in metadata: %s", fileName)
}

// UpdateFileSize updates the size of a file in metadata
func (mm *MetadataManager) UpdateFileSize(tableName string, fileName string, newSize int64) error {
	metadata, err := mm.LoadTableMetadata(tableName)
	if err != nil {
		return fmt.Errorf("failed to load metadata: %w", err)
	}

	for i, file := range metadata.Files {
		if file.Name == fileName {
			oldSize := metadata.Files[i].Size
			metadata.Files[i].Size = newSize
			metadata.Files[i].Modified = time.Now()
			metadata.TotalSize = metadata.TotalSize - oldSize + newSize
			metadata.LastModified = time.Now()

			return mm.SaveTableMetadata(tableName, metadata)
		}
	}

	return fmt.Errorf("file not found in metadata: %s", fileName)
}

// ListTables returns a list of all tables
func (mm *MetadataManager) ListTables() ([]string, error) {
	tablesPath := filepath.Join(mm.BasePath, "tables")

	entries, err := os.ReadDir(tablesPath)
	if err != nil {
		if os.IsNotExist(err) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("failed to read tables directory: %w", err)
	}

	var tables []string
	for _, entry := range entries {
		if entry.IsDir() {
			tables = append(tables, entry.Name())
		}
	}

	return tables, nil
}

// TableExists checks if a table exists
func (mm *MetadataManager) TableExists(tableName string) bool {
	metadataPath := getMetadataFilePath(mm.BasePath, tableName)
	_, err := os.Stat(metadataPath)
	return err == nil
}

// ============================================================================
// INTERNAL PATH UTILITIES (moved from utils to storage package)
// ============================================================================

// getTablePath returns the full path for a table
func getTablePath(basePath, tableName string) string {
	return filepath.Join(basePath, "tables", tableName)
}

// getDataFilePath returns the full path for a data file
func getDataFilePath(basePath, tableName, fileName string) string {
	return filepath.Join(getTablePath(basePath, tableName), fileName)
}

// getSchemaFilePath returns the full path for a table schema file
func getSchemaFilePath(basePath, tableName string) string {
	return filepath.Join(getTablePath(basePath, tableName), "schema.json")
}

// getMetadataFilePath returns the full path for a table metadata file
func getMetadataFilePath(basePath, tableName string) string {
	return filepath.Join(getTablePath(basePath, tableName), "metadata.json")
}
