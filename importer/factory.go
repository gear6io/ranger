package importer

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go/table"
)

// ImporterType represents the type of importer
type ImporterType string

const (
	ImporterTypeParquet ImporterType = "parquet"
	ImporterTypeAvro    ImporterType = "avro"
)

// Importer defines the interface for file importers
type Importer interface {
	// InferSchema reads a file and infers the schema
	InferSchema(filePath string) (*Schema, *FileStats, error)

	// GetTableLocation returns the location where table data would be stored
	GetTableLocation(tableIdent table.Identifier) string

	// ImportTable imports a file into an Iceberg table
	ImportTable(ctx context.Context, req ImportRequest) (*ImportResult, error)

	// Close closes the importer and releases resources
	Close() error
}

// ImporterFactory creates importers based on file type
type ImporterFactory struct {
	config *config.Config
}

// NewImporterFactory creates a new importer factory
func NewImporterFactory(cfg *config.Config) *ImporterFactory {
	return &ImporterFactory{
		config: cfg,
	}
}

// CreateImporter creates an importer based on the file extension
func (f *ImporterFactory) CreateImporter(filePath string) (Importer, ImporterType, error) {
	ext := strings.ToLower(filepath.Ext(filePath))

	switch ext {
	case ".parquet":
		importer, err := NewParquetImporter(f.config)
		if err != nil {
			return nil, ImporterTypeParquet, fmt.Errorf("failed to create Parquet importer: %w", err)
		}
		return importer, ImporterTypeParquet, nil

	case ".avro":
		importer, err := NewAvroImporter(f.config)
		if err != nil {
			return nil, ImporterTypeAvro, fmt.Errorf("failed to create Avro importer: %w", err)
		}
		return importer, ImporterTypeAvro, nil

	default:
		return nil, "", fmt.Errorf("unsupported file format: %s (supported: .parquet, .avro)", ext)
	}
}

// CreateImporterByType creates an importer for a specific type
func (f *ImporterFactory) CreateImporterByType(importerType ImporterType) (Importer, error) {
	switch importerType {
	case ImporterTypeParquet:
		return NewParquetImporter(f.config)

	case ImporterTypeAvro:
		return NewAvroImporter(f.config)

	default:
		return nil, fmt.Errorf("unsupported importer type: %s", importerType)
	}
}

// GetSupportedFormats returns a list of supported file formats
func (f *ImporterFactory) GetSupportedFormats() []string {
	return []string{".parquet", ".avro"}
}

// DetectFileType detects the file type based on file extension
func (f *ImporterFactory) DetectFileType(filePath string) (ImporterType, error) {
	ext := strings.ToLower(filepath.Ext(filePath))

	switch ext {
	case ".parquet":
		return ImporterTypeParquet, nil
	case ".avro":
		return ImporterTypeAvro, nil
	default:
		return "", fmt.Errorf("unsupported file format: %s", ext)
	}
}
