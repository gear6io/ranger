package registry

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/TFMV/icebox/server/metadata/registry/regtypes"
)

// TableReference represents a simple table reference
type TableReference struct {
	Database string `json:"database"`
	Table    string `json:"table"`
}

// TableMetadata represents a composite view of table metadata for storage operations
// This combines information from multiple tables for convenience
type TableMetadata struct {
	Database      string                `json:"database"`
	Name          string                `json:"name"`
	Schema        []byte                `json:"schema"`
	StorageEngine string                `json:"storage_engine"`
	EngineConfig  string                `json:"engine_config"`
	FileCount     int                   `json:"file_count"`
	TotalSize     int64                 `json:"total_size"`
	LastModified  time.Time             `json:"last_modified"`
	Created       time.Time             `json:"created"`
	Files         []*regtypes.TableFile `json:"files"`
}

// CompleteTableInfo represents a complete table with all its metadata and lazy loading
type CompleteTableInfo struct {
	*regtypes.Table
	Database string

	// Storage metadata
	StorageInfo *regtypes.TableMetadata

	// Lazy-loaded fields
	columns     []*regtypes.TableColumn
	partitions  []*regtypes.TablePartition
	indexes     []*regtypes.TableIndex
	constraints []*regtypes.TableConstraint
	statistics  []*regtypes.TableStatistic

	// Lazy loading control
	loadedFields map[string]bool
	mu           sync.RWMutex
	db           *sql.DB
	tableID      int64
}

// initializeLazyFields sets up the lazy loading infrastructure
func (cti *CompleteTableInfo) initializeLazyFields(db *sql.DB, tableID int64) {
	cti.db = db
	cti.tableID = tableID
	cti.loadedFields = make(map[string]bool)
}

// GetColumns returns table columns, loading them if needed
func (cti *CompleteTableInfo) GetColumns(ctx context.Context) ([]*regtypes.TableColumn, error) {
	cti.mu.RLock()
	if cti.loadedFields["columns"] {
		cti.mu.RUnlock()
		return cti.columns, nil
	}
	cti.mu.RUnlock()

	// Load columns
	cti.mu.Lock()
	defer cti.mu.Unlock()

	// Double-check after acquiring write lock
	if cti.loadedFields["columns"] {
		return cti.columns, nil
	}

	columns, err := cti.loadTableColumns(ctx)
	if err != nil {
		return nil, err
	}

	cti.columns = columns
	cti.loadedFields["columns"] = true
	return columns, nil
}

// GetPartitions returns table partitions, loading them if needed
func (cti *CompleteTableInfo) GetPartitions(ctx context.Context) ([]*regtypes.TablePartition, error) {
	cti.mu.RLock()
	if cti.loadedFields["partitions"] {
		cti.mu.RUnlock()
		return cti.partitions, nil
	}
	cti.mu.RUnlock()

	// Load partitions
	cti.mu.Lock()
	defer cti.mu.Unlock()

	// Double-check after acquiring write lock
	if cti.loadedFields["partitions"] {
		return cti.partitions, nil
	}

	partitions, err := cti.loadTablePartitions(ctx)
	if err != nil {
		return nil, err
	}

	cti.partitions = partitions
	cti.loadedFields["partitions"] = true
	return partitions, nil
}

// GetIndexes returns table indexes, loading them if needed
func (cti *CompleteTableInfo) GetIndexes(ctx context.Context) ([]*regtypes.TableIndex, error) {
	cti.mu.RLock()
	if cti.loadedFields["indexes"] {
		cti.mu.RUnlock()
		return cti.indexes, nil
	}
	cti.mu.RUnlock()

	// Load indexes
	cti.mu.Lock()
	defer cti.mu.Unlock()

	// Double-check after acquiring write lock
	if cti.loadedFields["indexes"] {
		return cti.indexes, nil
	}

	indexes, err := cti.loadTableIndexes(ctx)
	if err != nil {
		return nil, err
	}

	cti.indexes = indexes
	cti.loadedFields["indexes"] = true
	return indexes, nil
}

// GetConstraints returns table constraints, loading them if needed
func (cti *CompleteTableInfo) GetConstraints(ctx context.Context) ([]*regtypes.TableConstraint, error) {
	cti.mu.RLock()
	if cti.loadedFields["constraints"] {
		cti.mu.RUnlock()
		return cti.constraints, nil
	}
	cti.mu.RUnlock()

	// Load constraints
	cti.mu.Lock()
	defer cti.mu.Unlock()

	// Double-check after acquiring write lock
	if cti.loadedFields["constraints"] {
		return cti.constraints, nil
	}

	constraints, err := cti.loadTableConstraints(ctx)
	if err != nil {
		return nil, err
	}

	cti.constraints = constraints
	cti.loadedFields["constraints"] = true
	return constraints, nil
}

// GetStatistics returns table statistics, loading them if needed
func (cti *CompleteTableInfo) GetStatistics(ctx context.Context) ([]*regtypes.TableStatistic, error) {
	cti.mu.RLock()
	if cti.loadedFields["statistics"] {
		cti.mu.RUnlock()
		return cti.statistics, nil
	}
	cti.mu.RUnlock()

	// Load statistics
	cti.mu.Lock()
	defer cti.mu.Unlock()

	// Double-check after acquiring write lock
	if cti.loadedFields["statistics"] {
		return cti.statistics, nil
	}

	statistics, err := cti.loadTableStatistics(ctx)
	if err != nil {
		return nil, err
	}

	cti.statistics = statistics
	cti.loadedFields["statistics"] = true
	return statistics, nil
}

// IsFieldLoaded checks if a specific field has been loaded
func (cti *CompleteTableInfo) IsFieldLoaded(fieldName string) bool {
	cti.mu.RLock()
	defer cti.mu.RUnlock()
	return cti.loadedFields[fieldName]
}

// PreloadFields loads multiple fields at once for better performance
func (cti *CompleteTableInfo) PreloadFields(ctx context.Context, fields []string) error {
	for _, field := range fields {
		switch field {
		case "columns":
			if _, err := cti.GetColumns(ctx); err != nil {
				return err
			}
		case "partitions":
			if _, err := cti.GetPartitions(ctx); err != nil {
				return err
			}
		case "indexes":
			if _, err := cti.GetIndexes(ctx); err != nil {
				return err
			}
		case "constraints":
			if _, err := cti.GetConstraints(ctx); err != nil {
				return err
			}
		case "statistics":
			if _, err := cti.GetStatistics(ctx); err != nil {
				return err
			}
		}
	}
	return nil
}

// Helper methods for loading related table data
func (cti *CompleteTableInfo) loadTableColumns(ctx context.Context) ([]*regtypes.TableColumn, error) {
	query := `
		SELECT id, table_id, column_name, column_type, is_nullable, column_default, column_comment
		FROM table_columns
		WHERE table_id = ?
		ORDER BY ordinal_position
	`

	rows, err := cti.db.QueryContext(ctx, query, cti.tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []*regtypes.TableColumn
	for rows.Next() {
		var column regtypes.TableColumn
		err := rows.Scan(
			&column.ID, &column.TableID, &column.ColumnName, &column.DataType,
			&column.IsNullable, &column.DefaultValue, &column.Description,
		)
		if err != nil {
			return nil, err
		}
		columns = append(columns, &column)
	}

	return columns, nil
}

func (cti *CompleteTableInfo) loadTablePartitions(ctx context.Context) ([]*regtypes.TablePartition, error) {
	query := `
		SELECT id, table_id, partition_key, partition_path, row_count, file_count, total_size, min_values, max_values, created_at, updated_at
		FROM table_partitions
		WHERE table_id = ?
		ORDER BY partition_key
	`

	rows, err := cti.db.QueryContext(ctx, query, cti.tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var partitions []*regtypes.TablePartition
	for rows.Next() {
		var partition regtypes.TablePartition
		err := rows.Scan(
			&partition.ID, &partition.TableID, &partition.PartitionKey, &partition.PartitionPath,
			&partition.RowCount, &partition.FileCount, &partition.TotalSize,
			&partition.MinValues, &partition.MaxValues, &partition.CreatedAt, &partition.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		partitions = append(partitions, &partition)
	}

	return partitions, nil
}

func (cti *CompleteTableInfo) loadTableIndexes(ctx context.Context) ([]*regtypes.TableIndex, error) {
	query := `
		SELECT id, table_id, index_name, index_type, columns, is_unique, is_primary, is_active, created_at, updated_at
		FROM table_indexes
		WHERE table_id = ? AND is_active = true
		ORDER BY index_name
	`

	rows, err := cti.db.QueryContext(ctx, query, cti.tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []*regtypes.TableIndex
	for rows.Next() {
		var index regtypes.TableIndex
		err := rows.Scan(
			&index.ID, &index.TableID, &index.IndexName, &index.IndexType, &index.Columns,
			&index.IsUnique, &index.IsPrimary, &index.IsActive, &index.CreatedAt, &index.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, &index)
	}

	return indexes, nil
}

func (cti *CompleteTableInfo) loadTableConstraints(ctx context.Context) ([]*regtypes.TableConstraint, error) {
	query := `
		SELECT id, table_id, constraint_name, constraint_type, columns, reference_table, reference_columns, created_at, updated_at
		FROM table_constraints
		WHERE table_id = ?
		ORDER BY constraint_name
	`

	rows, err := cti.db.QueryContext(ctx, query, cti.tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []*regtypes.TableConstraint
	for rows.Next() {
		var constraint regtypes.TableConstraint
		err := rows.Scan(
			&constraint.ID, &constraint.TableID, &constraint.ConstraintName, &constraint.ConstraintType,
			&constraint.Columns, &constraint.ReferenceTable, &constraint.ReferenceColumns,
			&constraint.CreatedAt, &constraint.UpdatedAt,
		)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, &constraint)
	}

	return constraints, nil
}

func (cti *CompleteTableInfo) loadTableStatistics(ctx context.Context) ([]*regtypes.TableStatistic, error) {
	query := `
		SELECT id, table_id, column_name, stat_value, created_at, last_updated
		FROM table_statistics
		WHERE table_id = ?
		ORDER BY column_name
	`

	rows, err := cti.db.QueryContext(ctx, query, cti.tableID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statistics []*regtypes.TableStatistic
	for rows.Next() {
		var statistic regtypes.TableStatistic
		err := rows.Scan(
			&statistic.ID, &statistic.TableID, &statistic.ColumnName, &statistic.StatValue,
			&statistic.CreatedAt, &statistic.LastUpdated,
		)
		if err != nil {
			return nil, err
		}
		statistics = append(statistics, &statistic)
	}

	return statistics, nil
}
