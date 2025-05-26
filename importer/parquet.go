package importer

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/fs/local"
	"github.com/TFMV/icebox/tableops"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Schema represents a simplified table schema
type Schema struct {
	Fields []Field `json:"fields"`
}

// Field represents a single column in a schema
type Field struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// FileStats contains statistics about a Parquet file
type FileStats struct {
	RecordCount int64 `json:"record_count"`
	FileSize    int64 `json:"file_size"`
	ColumnCount int   `json:"column_count"`
}

// ImportRequest contains all parameters for importing a table
type ImportRequest struct {
	ParquetFile    string
	TableIdent     table.Identifier
	NamespaceIdent table.Identifier
	Schema         *Schema
	Overwrite      bool
	PartitionBy    []string
}

// ImportResult contains the results of a table import
type ImportResult struct {
	TableIdent    table.Identifier
	RecordCount   int64
	DataSize      int64
	TableLocation string
}

// ParquetImporter handles importing Parquet files into Iceberg tables
type ParquetImporter struct {
	config    *config.Config
	catalog   catalog.CatalogInterface
	allocator memory.Allocator
	writer    *tableops.Writer
}

// NewParquetImporter creates a new Parquet importer
func NewParquetImporter(cfg *config.Config) (*ParquetImporter, error) {
	// Create catalog using the factory
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	// Create table writer for data operations
	writer := tableops.NewWriter(cat)

	return &ParquetImporter{
		config:    cfg,
		catalog:   cat,
		allocator: memory.NewGoAllocator(),
		writer:    writer,
	}, nil
}

// Close closes the importer and releases resources
func (p *ParquetImporter) Close() error {
	if p.catalog != nil {
		return p.catalog.Close()
	}
	return nil
}

// InferSchema reads a Parquet file and infers the schema
func (p *ParquetImporter) InferSchema(parquetFile string) (*Schema, *FileStats, error) {
	// Get file stats
	fileInfo, err := os.Stat(parquetFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to stat file: %w", err)
	}

	// Read Parquet file to get schema and metadata
	arrowSchema, recordCount, err := p.readParquetSchema(parquetFile)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read Parquet schema: %w", err)
	}

	// Convert Arrow schema to our simplified schema format
	schema := p.convertArrowSchemaToSimple(arrowSchema)

	stats := &FileStats{
		RecordCount: recordCount,
		FileSize:    fileInfo.Size(),
		ColumnCount: len(schema.Fields),
	}

	return schema, stats, nil
}

// GetTableLocation returns the location where table data would be stored
func (p *ParquetImporter) GetTableLocation(tableIdent table.Identifier) string {
	if p.config.Storage.FileSystem == nil {
		return ""
	}

	// Build path: warehouse/namespace/table_name
	path := p.config.Storage.FileSystem.RootPath
	for _, part := range tableIdent {
		path = filepath.Join(path, part)
	}

	return "file://" + filepath.ToSlash(path)
}

// ImportTable imports a Parquet file into an Iceberg table
func (p *ParquetImporter) ImportTable(ctx context.Context, req ImportRequest) (*ImportResult, error) {
	// 1. Create namespace if it doesn't exist
	exists, err := p.catalog.CheckNamespaceExists(ctx, req.NamespaceIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to check namespace existence: %w", err)
	}

	if !exists {
		err = p.catalog.CreateNamespace(ctx, req.NamespaceIdent, iceberg.Properties{
			"description": "Auto-created namespace for Parquet import",
		})
		if err != nil {
			return nil, fmt.Errorf("failed to create namespace: %w", err)
		}
		fmt.Printf("✅ Created namespace: %v\n", req.NamespaceIdent)
	}

	// 2. Check if table exists
	tableExists, err := p.catalog.CheckTableExists(ctx, req.TableIdent)
	if err != nil {
		return nil, fmt.Errorf("failed to check table existence: %w", err)
	}

	if tableExists {
		if !req.Overwrite {
			return nil, fmt.Errorf("table %v already exists (use --overwrite to replace)", req.TableIdent)
		}

		// Drop existing table
		err = p.catalog.DropTable(ctx, req.TableIdent)
		if err != nil {
			return nil, fmt.Errorf("failed to drop existing table: %w", err)
		}
		fmt.Printf("🗑️  Dropped existing table: %v\n", req.TableIdent)
	}

	// 3. Read the Parquet file to get the proper Arrow schema
	arrowTable, err := p.readParquetFile(ctx, req.ParquetFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read Parquet file: %w", err)
	}
	defer arrowTable.Release()

	// 4. Convert Arrow schema to Iceberg schema
	icebergSchema, err := p.convertArrowSchemaToIceberg(arrowTable.Schema())
	if err != nil {
		return nil, fmt.Errorf("failed to convert schema to Iceberg format: %w", err)
	}

	// 5. Create the Iceberg table
	icebergTable, err := p.catalog.CreateTable(ctx, req.TableIdent, icebergSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}
	fmt.Printf("✅ Created table: %v\n", req.TableIdent)

	// 6. Write the data to the table using tableops writer
	writeOpts := tableops.DefaultWriteOptions()
	writeOpts.SnapshotProperties["icebox.import.source"] = req.ParquetFile

	// Get file info for metadata
	fileInfo, err := os.Stat(req.ParquetFile)
	if err == nil {
		writeOpts.SnapshotProperties["icebox.import.timestamp"] = fmt.Sprintf("%d", fileInfo.ModTime().Unix())
	}

	err = p.writer.WriteArrowTable(ctx, icebergTable, arrowTable, writeOpts)
	if err != nil {
		return nil, fmt.Errorf("failed to write data to table: %w", err)
	}

	// 7. Get table location and file info for result
	tableLocation := p.GetTableLocation(req.TableIdent)

	// Re-read file info if not already available
	if fileInfo == nil {
		fileInfo, _ = os.Stat(req.ParquetFile)
	}

	fmt.Printf("📁 Copied data to: %s\n", tableLocation)

	return &ImportResult{
		TableIdent:    req.TableIdent,
		RecordCount:   arrowTable.NumRows(),
		DataSize:      fileInfo.Size(),
		TableLocation: tableLocation,
	}, nil
}

// readParquetSchema reads the schema and metadata from a Parquet file without loading all data
func (p *ParquetImporter) readParquetSchema(parquetFile string) (*arrow.Schema, int64, error) {
	// Open the Parquet file
	f, err := os.Open(parquetFile)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Create parquet reader
	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer parquetReader.Close()

	// Create Arrow reader to get schema
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, p.allocator)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Get the schema without reading data
	schema, err := arrowReader.Schema()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get arrow schema: %w", err)
	}

	// Get record count from Parquet metadata
	recordCount := int64(0)
	for i := 0; i < parquetReader.NumRowGroups(); i++ {
		rowGroup := parquetReader.RowGroup(i)
		recordCount += rowGroup.NumRows()
	}

	return schema, recordCount, nil
}

// readParquetFile reads a Parquet file and returns an Arrow table
func (p *ParquetImporter) readParquetFile(ctx context.Context, path string) (arrow.Table, error) {
	// Remove file:// prefix if present
	localPath := path
	if strings.HasPrefix(path, "file://") {
		localPath = path[7:]
	}

	// Ensure the file exists
	exists, err := local.NewFileSystem("").Exists(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to check file existence: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("parquet file does not exist: %s", localPath)
	}

	// Open the Parquet file
	f, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	// Create parquet reader
	parquetReader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}
	defer parquetReader.Close()

	// Create Arrow reader with optimized batch size
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{
		BatchSize: 10000, // Larger batch size for better performance
	}, p.allocator)
	if err != nil {
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	// Read the entire table
	arrowTable, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to read arrow table: %w", err)
	}

	return arrowTable, nil
}

// convertArrowSchemaToSimple converts an Arrow schema to our simplified schema format
func (p *ParquetImporter) convertArrowSchemaToSimple(arrowSchema *arrow.Schema) *Schema {
	fields := make([]Field, 0, len(arrowSchema.Fields()))

	for _, field := range arrowSchema.Fields() {
		simpleType := p.arrowTypeToSimpleType(field.Type)
		fields = append(fields, Field{
			Name:     field.Name,
			Type:     simpleType,
			Nullable: field.Nullable,
		})
	}

	return &Schema{Fields: fields}
}

// convertArrowSchemaToIceberg converts an Arrow schema to an Iceberg schema
func (p *ParquetImporter) convertArrowSchemaToIceberg(arrowSchema *arrow.Schema) (*iceberg.Schema, error) {
	fields := make([]iceberg.NestedField, 0, len(arrowSchema.Fields()))

	for i, field := range arrowSchema.Fields() {
		icebergType, err := p.arrowTypeToIcebergType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}

		icebergField := iceberg.NestedField{
			ID:       i + 1, // Iceberg field IDs start at 1
			Name:     field.Name,
			Type:     icebergType,
			Required: !field.Nullable,
		}
		fields = append(fields, icebergField)
	}

	return iceberg.NewSchema(1, fields...), nil
}

// arrowTypeToSimpleType converts Arrow data types to simple string representations
func (p *ParquetImporter) arrowTypeToSimpleType(arrowType arrow.DataType) string {
	switch arrowType.ID() {
	case arrow.BOOL:
		return "boolean"
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return "int"
	case arrow.INT64:
		return "long"
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		return "int"
	case arrow.UINT64:
		return "long"
	case arrow.FLOAT32:
		return "float"
	case arrow.FLOAT64:
		return "double"
	case arrow.STRING, arrow.LARGE_STRING:
		return "string"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "binary"
	case arrow.DATE32, arrow.DATE64:
		return "date"
	case arrow.TIMESTAMP:
		return "timestamp"
	case arrow.TIME32, arrow.TIME64:
		return "time"
	case arrow.DECIMAL128, arrow.DECIMAL256:
		return "decimal"
	case arrow.FIXED_SIZE_BINARY:
		return "fixed"
	default:
		return "string" // Default fallback
	}
}

// arrowTypeToIcebergType converts Arrow data types to Iceberg data types
func (p *ParquetImporter) arrowTypeToIcebergType(arrowType arrow.DataType) (iceberg.Type, error) {
	switch arrowType.ID() {
	case arrow.BOOL:
		return iceberg.PrimitiveTypes.Bool, nil
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return iceberg.PrimitiveTypes.Int32, nil
	case arrow.INT64:
		return iceberg.PrimitiveTypes.Int64, nil
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		return iceberg.PrimitiveTypes.Int32, nil
	case arrow.UINT64:
		return iceberg.PrimitiveTypes.Int64, nil
	case arrow.FLOAT32:
		return iceberg.PrimitiveTypes.Float32, nil
	case arrow.FLOAT64:
		return iceberg.PrimitiveTypes.Float64, nil
	case arrow.STRING, arrow.LARGE_STRING:
		return iceberg.PrimitiveTypes.String, nil
	case arrow.BINARY, arrow.LARGE_BINARY:
		return iceberg.PrimitiveTypes.Binary, nil
	case arrow.DATE32, arrow.DATE64:
		return iceberg.PrimitiveTypes.Date, nil
	case arrow.TIMESTAMP:
		return iceberg.PrimitiveTypes.Timestamp, nil
	case arrow.TIME32, arrow.TIME64:
		return iceberg.PrimitiveTypes.Time, nil
	case arrow.DECIMAL128:
		if dt, ok := arrowType.(*arrow.Decimal128Type); ok {
			return iceberg.DecimalTypeOf(int(dt.Precision), int(dt.Scale)), nil
		}
		return iceberg.DecimalTypeOf(38, 18), nil // Default precision/scale
	case arrow.DECIMAL256:
		if dt, ok := arrowType.(*arrow.Decimal256Type); ok {
			return iceberg.DecimalTypeOf(int(dt.Precision), int(dt.Scale)), nil
		}
		return iceberg.DecimalTypeOf(38, 18), nil // Default precision/scale
	case arrow.FIXED_SIZE_BINARY:
		if dt, ok := arrowType.(*arrow.FixedSizeBinaryType); ok {
			return iceberg.FixedTypeOf(dt.ByteWidth), nil
		}
		return iceberg.FixedTypeOf(16), nil // Default size
	default:
		// For unsupported types, fallback to string
		return iceberg.PrimitiveTypes.String, nil
	}
}
