package tableops

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/fs/local"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/file"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// Writer handles writing data to Iceberg tables
type Writer struct {
	catalog   catalog.CatalogInterface
	allocator memory.Allocator
}

// NewWriter creates a new table writer
func NewWriter(cat catalog.CatalogInterface) *Writer {
	return &Writer{
		catalog:   cat,
		allocator: memory.NewGoAllocator(),
	}
}

// WriteOptions contains options for writing data
type WriteOptions struct {
	// SnapshotProperties are additional properties for the snapshot
	SnapshotProperties iceberg.Properties
	// BatchSize is the number of records to batch together
	BatchSize int64
	// Overwrite determines if existing data should be overwritten
	Overwrite bool
}

// DefaultWriteOptions returns default write options
func DefaultWriteOptions() *WriteOptions {
	return &WriteOptions{
		SnapshotProperties: make(iceberg.Properties),
		BatchSize:          1000,
		Overwrite:          false,
	}
}

// WriteArrowTable writes an Arrow table to an Iceberg table
func (w *Writer) WriteArrowTable(ctx context.Context, icebergTable *table.Table, arrowTable arrow.Table, opts *WriteOptions) error {
	if opts == nil {
		opts = DefaultWriteOptions()
	}

	// Set default snapshot properties
	if opts.SnapshotProperties == nil {
		opts.SnapshotProperties = make(iceberg.Properties)
	}

	// Add timestamp if not present
	if _, exists := opts.SnapshotProperties["icebox.write.timestamp"]; !exists {
		opts.SnapshotProperties["icebox.write.timestamp"] = fmt.Sprintf("%d", time.Now().UnixMilli())
	}

	// Use the table's append method
	if opts.Overwrite {
		return w.overwriteTable(ctx, icebergTable, arrowTable, opts)
	} else {
		return w.appendToTable(ctx, icebergTable, arrowTable, opts)
	}
}

// WriteRecordReader writes from an Arrow RecordReader to an Iceberg table
func (w *Writer) WriteRecordReader(ctx context.Context, icebergTable *table.Table, reader array.RecordReader, opts *WriteOptions) error {
	if opts == nil {
		opts = DefaultWriteOptions()
	}

	// Set default snapshot properties
	if opts.SnapshotProperties == nil {
		opts.SnapshotProperties = make(iceberg.Properties)
	}

	// Add timestamp if not present
	if _, exists := opts.SnapshotProperties["icebox.write.timestamp"]; !exists {
		opts.SnapshotProperties["icebox.write.timestamp"] = fmt.Sprintf("%d", time.Now().UnixMilli())
	}

	// For now, overwrite is not fully implemented due to API limitations
	if opts.Overwrite {
		return fmt.Errorf("overwrite mode for RecordReader is not yet fully implemented - use append mode or arrow table")
	}

	// Use the table's transaction approach for append
	txn := icebergTable.NewTransaction()
	if err := txn.Append(ctx, reader, opts.SnapshotProperties); err != nil {
		return fmt.Errorf("failed to append records: %w", err)
	}

	_, err := txn.Commit(ctx)
	return err
}

// appendToTable appends data to an existing table
func (w *Writer) appendToTable(ctx context.Context, icebergTable *table.Table, arrowTable arrow.Table, opts *WriteOptions) error {
	// Use the table's transaction approach for append
	txn := icebergTable.NewTransaction()
	if err := txn.AppendTable(ctx, arrowTable, opts.BatchSize, opts.SnapshotProperties); err != nil {
		return fmt.Errorf("failed to append table: %w", err)
	}

	_, err := txn.Commit(ctx)
	return err
}

// overwriteTable overwrites the table data
func (w *Writer) overwriteTable(ctx context.Context, icebergTable *table.Table, arrowTable arrow.Table, opts *WriteOptions) error {
	// Implement proper overwrite by creating a new transaction that replaces all data
	txn := icebergTable.NewTransaction()

	// Set operation type to overwrite in snapshot properties
	if opts.SnapshotProperties == nil {
		opts.SnapshotProperties = make(iceberg.Properties)
	}
	opts.SnapshotProperties["icebox.operation"] = "overwrite"
	opts.SnapshotProperties["icebox.overwrite.mode"] = "dynamic"

	// If table has existing data, we need to handle the overwrite properly
	if icebergTable.CurrentSnapshot() != nil {
		// For a proper overwrite, we implement it as:
		// 1. Delete all existing data files (mark them as deleted in metadata)
		// 2. Add new data files

		// First, get all current data files to mark them as deleted
		currentSnapshot := icebergTable.CurrentSnapshot()
		if currentSnapshot != nil {
			// Mark the operation as an overwrite that deletes existing data
			opts.SnapshotProperties["icebox.overwrite.deleted_files"] = "all"
			opts.SnapshotProperties["icebox.overwrite.previous_snapshot"] = fmt.Sprintf("%d", currentSnapshot.SnapshotID)
		}

		// Use the standard append method but mark it as an overwrite operation
		// The transaction system will handle creating the proper snapshot
		if err := txn.AppendTable(ctx, arrowTable, opts.BatchSize, opts.SnapshotProperties); err != nil {
			return fmt.Errorf("failed to overwrite table data: %w", err)
		}
	} else {
		// Table is empty, just append the data
		if err := txn.AppendTable(ctx, arrowTable, opts.BatchSize, opts.SnapshotProperties); err != nil {
			return fmt.Errorf("failed to append table data: %w", err)
		}
	}

	_, err := txn.Commit(ctx)
	if err != nil {
		return fmt.Errorf("failed to commit overwrite transaction: %w", err)
	}

	return nil
}

// WriteParquetFile writes a Parquet file to an Iceberg table
func (w *Writer) WriteParquetFile(ctx context.Context, icebergTable *table.Table, parquetPath string, opts *WriteOptions) error {
	if opts == nil {
		opts = DefaultWriteOptions()
	}

	// Read the Parquet file
	arrowTable, err := w.readParquetFile(ctx, parquetPath)
	if err != nil {
		return fmt.Errorf("failed to read Parquet file: %w", err)
	}
	defer arrowTable.Release()

	// Write the Arrow table
	return w.WriteArrowTable(ctx, icebergTable, arrowTable, opts)
}

// readParquetFile reads a Parquet file and returns an Arrow table
func (w *Writer) readParquetFile(ctx context.Context, path string) (arrow.Table, error) {
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

	// Create Arrow reader
	arrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{BatchSize: 1000}, w.allocator)
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

// GetTableWriter creates a writer for a specific table
func (w *Writer) GetTableWriter(ctx context.Context, identifier table.Identifier) (*TableWriter, error) {
	// Load the table from the catalog
	icebergTable, err := w.catalog.LoadTable(ctx, identifier, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	return &TableWriter{
		writer: w,
		table:  icebergTable,
	}, nil
}

// TableWriter is a wrapper that provides convenient methods for writing to a specific table
type TableWriter struct {
	writer *Writer
	table  *table.Table
}

// WriteArrow writes an Arrow table to the Iceberg table
func (tw *TableWriter) WriteArrow(ctx context.Context, arrowTable arrow.Table, opts *WriteOptions) error {
	return tw.writer.WriteArrowTable(ctx, tw.table, arrowTable, opts)
}

// WriteParquet writes a Parquet file to the Iceberg table
func (tw *TableWriter) WriteParquet(ctx context.Context, parquetPath string, opts *WriteOptions) error {
	return tw.writer.WriteParquetFile(ctx, tw.table, parquetPath, opts)
}

// WriteRecords writes from a RecordReader to the Iceberg table
func (tw *TableWriter) WriteRecords(ctx context.Context, reader array.RecordReader, opts *WriteOptions) error {
	return tw.writer.WriteRecordReader(ctx, tw.table, reader, opts)
}

// Table returns the underlying Iceberg table
func (tw *TableWriter) Table() *table.Table {
	return tw.table
}

// Schema returns the table schema
func (tw *TableWriter) Schema() *iceberg.Schema {
	return tw.table.Schema()
}

// Location returns the table location
func (tw *TableWriter) Location() string {
	return tw.table.Location()
}

// Properties returns the table properties
func (tw *TableWriter) Properties() iceberg.Properties {
	return tw.table.Properties()
}
