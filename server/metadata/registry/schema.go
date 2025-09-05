package registry

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
)

// RetrieveAllSchemas fires a single SQL query to get all schema data across all databases
// This is the kickass function that replaces the retriever approach
func (sm *Store) RetrieveAllSchemas(ctx context.Context) (map[string]*SchemaData, error) {
	// Single SQL query to get all schema data in one go
	query := `
		SELECT 
			d.name as database_name,
			t.name as table_name,
			t.id as table_id,
			c.id as column_id,
			c.column_name,
			c.display_name,
			c.data_type,
			c.is_nullable,
			c.is_primary,
			c.is_unique,
			c.default_value,
			c.description,
			c.ordinal_position,
			c.max_length,
			c.precision,
			c.scale,
			c.created_at as column_created_at,
			c.updated_at as column_updated_at,
			tm.schema_version,
			tm.storage_engine,
			tm.engine_config,
			tm.format,
			tm.compression,
			tm.partition_by,
			tm.sort_by,
			tm.settings,
			tm.table_config,
			tm.partition_order,
			tm.partition_strategy,
			tm.sort_order,
			tm.sort_strategy,
			tm.cache_enabled,
			tm.cache_size,
			tm.batch_size,
			tm.parallel_processing,
			tm.strict_validation,
			tm.strict_compliance,
			tm.allow_type_promotions,
			tm.max_schema_versions,
			tm.batch_validation_size,
			tm.max_validation_errors,
			tm.table_uuid,
			tm.format_version,
			tm.last_column_id,
			tm.last_partition_id,
			tm.last_modified as metadata_last_modified,
			tm.created_at as metadata_created_at,
			tm.updated_at as metadata_updated_at
		FROM databases d
		JOIN tables t ON d.id = t.database_id
		LEFT JOIN table_columns c ON t.id = c.table_id
		LEFT JOIN table_metadata tm ON t.id = tm.table_id
		WHERE d.deleted_at IS NULL AND t.deleted_at IS NULL
		ORDER BY d.name, t.name, c.ordinal_position
	`

	rows, err := sm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to retrieve all schemas", err)
	}
	defer rows.Close()

	schemas := make(map[string]*SchemaData)

	for rows.Next() {
		var (
			databaseName, tableName                                                                   string
			tableID                                                                                   int64
			columnID                                                                                  sql.NullInt64
			columnName, displayName, dataType, defaultValue, description                              sql.NullString
			isNullable, isPrimary, isUnique                                                           bool
			ordinalPosition                                                                           int
			maxLength, precision, scale                                                               sql.NullInt64
			columnCreatedAt, columnUpdatedAt                                                          time.Time
			schemaVersion                                                                             sql.NullInt64
			storageEngine, engineConfig, format, compression, partitionBy, sortBy, settings           sql.NullString
			tableConfig, partitionOrder, partitionStrategy, sortOrder, sortStrategy                   sql.NullString
			cacheEnabled, parallelProcessing, strictValidation, strictCompliance, allowTypePromotions bool
			cacheSize, batchSize, maxSchemaVersions, batchValidationSize, maxValidationErrors         sql.NullInt64
			tableUUID                                                                                 sql.NullString
			formatVersion, lastColumnID, lastPartitionID                                              sql.NullInt64
			metadataLastModified, metadataCreatedAt, metadataUpdatedAt                                sql.NullTime
		)

		err := rows.Scan(
			&databaseName, &tableName, &tableID,
			&columnID, &columnName, &displayName, &dataType, &isNullable, &isPrimary, &isUnique,
			&defaultValue, &description, &ordinalPosition, &maxLength, &precision, &scale,
			&columnCreatedAt, &columnUpdatedAt,
			&schemaVersion, &storageEngine, &engineConfig, &format, &compression, &partitionBy, &sortBy, &settings,
			&tableConfig, &partitionOrder, &partitionStrategy, &sortOrder, &sortStrategy,
			&cacheEnabled, &cacheSize, &batchSize, &parallelProcessing, &strictValidation, &strictCompliance, &allowTypePromotions,
			&maxSchemaVersions, &batchValidationSize, &maxValidationErrors, &tableUUID, &formatVersion, &lastColumnID, &lastPartitionID,
			&metadataLastModified, &metadataCreatedAt, &metadataUpdatedAt,
		)
		if err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan schema row", err)
		}

		// Create cache key
		cacheKey := fmt.Sprintf("%s.%s", databaseName, tableName)

		// Initialize schema data if not exists
		if _, exists := schemas[cacheKey]; !exists {
			schemas[cacheKey] = &SchemaData{
				Database: databaseName,
				Table:    tableName,
				TableID:  tableID,
				Columns:  make([]*regtypes.TableColumn, 0),
				Metadata: &regtypes.TableMetadata{
					TableID: tableID,
				},
			}
		}

		// Add column if it exists
		if columnID.Valid {
			column := &regtypes.TableColumn{
				ID:              int(columnID.Int64),
				TableID:         tableID,
				ColumnName:      columnName.String,
				DisplayName:     displayName.String,
				DataType:        dataType.String,
				IsNullable:      isNullable,
				IsPrimary:       isPrimary,
				IsUnique:        isUnique,
				DefaultValue:    defaultValue.String,
				Description:     description.String,
				OrdinalPosition: ordinalPosition,
			}
			column.CreatedAt = columnCreatedAt
			column.UpdatedAt = columnUpdatedAt

			if maxLength.Valid {
				column.MaxLength = int(maxLength.Int64)
			}
			if precision.Valid {
				column.Precision = int(precision.Int64)
			}
			if scale.Valid {
				column.Scale = int(scale.Int64)
			}

			schemas[cacheKey].Columns = append(schemas[cacheKey].Columns, column)
		}

		// Set metadata if not already set (only set once per table)
		if schemas[cacheKey].Metadata.SchemaVersion == 0 && schemaVersion.Valid {
			metadata := schemas[cacheKey].Metadata
			metadata.SchemaVersion = int(schemaVersion.Int64)
			metadata.StorageEngine = storageEngine.String
			metadata.EngineConfig = engineConfig.String
			metadata.Format = format.String
			metadata.Compression = compression.String
			metadata.PartitionBy = partitionBy.String
			metadata.SortBy = sortBy.String
			metadata.Settings = settings.String
			metadata.TableConfig = tableConfig.String
			metadata.PartitionOrder = partitionOrder.String
			metadata.PartitionStrategy = partitionStrategy.String
			metadata.SortOrder = sortOrder.String
			metadata.SortStrategy = sortStrategy.String
			metadata.CacheEnabled = cacheEnabled
			metadata.ParallelProcessing = parallelProcessing
			metadata.StrictValidation = strictValidation
			metadata.StrictCompliance = strictCompliance
			metadata.AllowTypePromotions = allowTypePromotions
			metadata.TableUUID = tableUUID.String

			if cacheSize.Valid {
				metadata.CacheSize = int(cacheSize.Int64)
			}
			if batchSize.Valid {
				metadata.BatchSize = int(batchSize.Int64)
			}
			if maxSchemaVersions.Valid {
				metadata.MaxSchemaVersions = int(maxSchemaVersions.Int64)
			}
			if batchValidationSize.Valid {
				metadata.BatchValidationSize = int(batchValidationSize.Int64)
			}
			if maxValidationErrors.Valid {
				metadata.MaxValidationErrors = int(maxValidationErrors.Int64)
			}
			if formatVersion.Valid {
				metadata.FormatVersion = int(formatVersion.Int64)
			}
			if lastColumnID.Valid {
				metadata.LastColumnID = int(lastColumnID.Int64)
			}
			if lastPartitionID.Valid {
				metadata.LastPartitionID = int(lastPartitionID.Int64)
			}
			if metadataLastModified.Valid {
				metadata.LastModified = metadataLastModified.Time
			}
			if metadataCreatedAt.Valid {
				metadata.CreatedAt = metadataCreatedAt.Time
			}
			if metadataUpdatedAt.Valid {
				metadata.UpdatedAt = metadataUpdatedAt.Time
			}
		}
	}

	if err = rows.Err(); err != nil {
		return nil, errors.New(errors.CommonInternal, "error iterating schema rows", err)
	}

	return schemas, nil
}

// RetrieveSchema loads schema for a specific table and database
// This is the direct function reference that will be passed to SchemaManager
func (sm *Store) RetrieveSchema(ctx context.Context, database, tableName string) (*SchemaData, error) {
	// Single SQL query to get schema data for specific table
	query := `
		SELECT 
			d.name as database_name,
			t.name as table_name,
			t.id as table_id,
			c.id as column_id,
			c.column_name,
			c.display_name,
			c.data_type,
			c.is_nullable,
			c.is_primary,
			c.is_unique,
			c.default_value,
			c.description,
			c.ordinal_position,
			c.max_length,
			c.precision,
			c.scale,
			c.created_at as column_created_at,
			c.updated_at as column_updated_at,
			tm.schema_version,
			tm.storage_engine,
			tm.engine_config,
			tm.format,
			tm.compression,
			tm.partition_by,
			tm.sort_by,
			tm.settings,
			tm.table_config,
			tm.partition_order,
			tm.partition_strategy,
			tm.sort_order,
			tm.sort_strategy,
			tm.cache_enabled,
			tm.cache_size,
			tm.batch_size,
			tm.parallel_processing,
			tm.strict_validation,
			tm.strict_compliance,
			tm.allow_type_promotions,
			tm.max_schema_versions,
			tm.batch_validation_size,
			tm.max_validation_errors,
			tm.table_uuid,
			tm.format_version,
			tm.last_column_id,
			tm.last_partition_id,
			tm.last_modified as metadata_last_modified,
			tm.created_at as metadata_created_at,
			tm.updated_at as metadata_updated_at
		FROM databases d
		JOIN tables t ON d.id = t.database_id
		LEFT JOIN table_columns c ON t.id = c.table_id
		LEFT JOIN table_metadata tm ON t.id = tm.table_id
		WHERE d.name = ? AND t.name = ? AND d.deleted_at IS NULL AND t.deleted_at IS NULL
		ORDER BY c.ordinal_position
	`

	rows, err := sm.db.QueryContext(ctx, query, database, tableName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to retrieve schema", err)
	}
	defer rows.Close()

	var schemaData *SchemaData

	for rows.Next() {
		var (
			databaseName, tableName                                                                   string
			tableID                                                                                   int64
			columnID                                                                                  sql.NullInt64
			columnName, displayName, dataType, defaultValue, description                              sql.NullString
			isNullable, isPrimary, isUnique                                                           bool
			ordinalPosition                                                                           int
			maxLength, precision, scale                                                               sql.NullInt64
			columnCreatedAt, columnUpdatedAt                                                          time.Time
			schemaVersion                                                                             sql.NullInt64
			storageEngine, engineConfig, format, compression, partitionBy, sortBy, settings           sql.NullString
			tableConfig, partitionOrder, partitionStrategy, sortOrder, sortStrategy                   sql.NullString
			cacheEnabled, parallelProcessing, strictValidation, strictCompliance, allowTypePromotions bool
			cacheSize, batchSize, maxSchemaVersions, batchValidationSize, maxValidationErrors         sql.NullInt64
			tableUUID                                                                                 sql.NullString
			formatVersion, lastColumnID, lastPartitionID                                              sql.NullInt64
			metadataLastModified, metadataCreatedAt, metadataUpdatedAt                                sql.NullTime
		)

		err := rows.Scan(
			&databaseName, &tableName, &tableID,
			&columnID, &columnName, &displayName, &dataType, &isNullable, &isPrimary, &isUnique,
			&defaultValue, &description, &ordinalPosition, &maxLength, &precision, &scale,
			&columnCreatedAt, &columnUpdatedAt,
			&schemaVersion, &storageEngine, &engineConfig, &format, &compression, &partitionBy, &sortBy, &settings,
			&tableConfig, &partitionOrder, &partitionStrategy, &sortOrder, &sortStrategy,
			&cacheEnabled, &cacheSize, &batchSize, &parallelProcessing, &strictValidation, &strictCompliance, &allowTypePromotions,
			&maxSchemaVersions, &batchValidationSize, &maxValidationErrors, &tableUUID, &formatVersion, &lastColumnID, &lastPartitionID,
			&metadataLastModified, &metadataCreatedAt, &metadataUpdatedAt,
		)
		if err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan schema row", err)
		}

		// Initialize schema data if not exists
		if schemaData == nil {
			schemaData = &SchemaData{
				Database: databaseName,
				Table:    tableName,
				TableID:  tableID,
				Columns:  make([]*regtypes.TableColumn, 0),
				Metadata: &regtypes.TableMetadata{
					TableID: tableID,
				},
			}
		}

		// Add column if it exists
		if columnID.Valid {
			column := &regtypes.TableColumn{
				ID:              int(columnID.Int64),
				TableID:         tableID,
				ColumnName:      columnName.String,
				DisplayName:     displayName.String,
				DataType:        dataType.String,
				IsNullable:      isNullable,
				IsPrimary:       isPrimary,
				IsUnique:        isUnique,
				DefaultValue:    defaultValue.String,
				Description:     description.String,
				OrdinalPosition: ordinalPosition,
			}
			column.CreatedAt = columnCreatedAt
			column.UpdatedAt = columnUpdatedAt

			if maxLength.Valid {
				column.MaxLength = int(maxLength.Int64)
			}
			if precision.Valid {
				column.Precision = int(precision.Int64)
			}
			if scale.Valid {
				column.Scale = int(scale.Int64)
			}

			schemaData.Columns = append(schemaData.Columns, column)
		}

		// Set metadata if not already set (only set once per table)
		if schemaData.Metadata.SchemaVersion == 0 && schemaVersion.Valid {
			metadata := schemaData.Metadata
			metadata.SchemaVersion = int(schemaVersion.Int64)
			metadata.StorageEngine = storageEngine.String
			metadata.EngineConfig = engineConfig.String
			metadata.Format = format.String
			metadata.Compression = compression.String
			metadata.PartitionBy = partitionBy.String
			metadata.SortBy = sortBy.String
			metadata.Settings = settings.String
			metadata.TableConfig = tableConfig.String
			metadata.PartitionOrder = partitionOrder.String
			metadata.PartitionStrategy = partitionStrategy.String
			metadata.SortOrder = sortOrder.String
			metadata.SortStrategy = sortStrategy.String
			metadata.CacheEnabled = cacheEnabled
			metadata.ParallelProcessing = parallelProcessing
			metadata.StrictValidation = strictValidation
			metadata.StrictCompliance = strictCompliance
			metadata.AllowTypePromotions = allowTypePromotions
			metadata.TableUUID = tableUUID.String

			if cacheSize.Valid {
				metadata.CacheSize = int(cacheSize.Int64)
			}
			if batchSize.Valid {
				metadata.BatchSize = int(batchSize.Int64)
			}
			if maxSchemaVersions.Valid {
				metadata.MaxSchemaVersions = int(maxSchemaVersions.Int64)
			}
			if batchValidationSize.Valid {
				metadata.BatchValidationSize = int(batchValidationSize.Int64)
			}
			if maxValidationErrors.Valid {
				metadata.MaxValidationErrors = int(maxValidationErrors.Int64)
			}
			if formatVersion.Valid {
				metadata.FormatVersion = int(formatVersion.Int64)
			}
			if lastColumnID.Valid {
				metadata.LastColumnID = int(lastColumnID.Int64)
			}
			if lastPartitionID.Valid {
				metadata.LastPartitionID = int(lastPartitionID.Int64)
			}
			if metadataLastModified.Valid {
				metadata.LastModified = metadataLastModified.Time
			}
			if metadataCreatedAt.Valid {
				metadata.CreatedAt = metadataCreatedAt.Time
			}
			if metadataUpdatedAt.Valid {
				metadata.UpdatedAt = metadataUpdatedAt.Time
			}
		}
	}

	if err = rows.Err(); err != nil {
		return nil, errors.New(errors.CommonInternal, "error iterating schema rows", err)
	}

	if schemaData == nil {
		return nil, errors.New(RegistryTableNotFound, "table not found", nil).
			AddContext("database", database).
			AddContext("table", tableName)
	}

	return schemaData, nil
}

// SchemaLoaderFunc is a function type for loading schemas
type SchemaLoaderFunc func(ctx context.Context, database, tableName string) (*iceberg.Schema, error)

// CreateSchemaDataLoader creates a schema data loader function that returns raw registry data
func (sm *Store) CreateSchemaDataLoader() func(ctx context.Context, database, tableName string) (*SchemaData, error) {
	return func(ctx context.Context, database, tableName string) (*SchemaData, error) {
		return sm.RetrieveSchema(ctx, database, tableName)
	}
}
