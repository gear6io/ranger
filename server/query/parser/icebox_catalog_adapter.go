package parser

import (
	"context"
	"fmt"
	"strings"

	"github.com/TFMV/icebox/server/catalog"
	"github.com/TFMV/icebox/server/config"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
)

// IceboxCatalogAdapter implements CatalogAdapter using Icebox's catalog system
type IceboxCatalogAdapter struct {
	catalog         catalog.CatalogInterface
	config          *config.Config
	currentDatabase string
	currentUser     string
}

// NewIceboxCatalogAdapter creates a new catalog adapter for Icebox
func NewIceboxCatalogAdapter(cfg *config.Config) (*IceboxCatalogAdapter, error) {
	catalogInstance, err := catalog.NewCatalog(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create catalog: %w", err)
	}

	return &IceboxCatalogAdapter{
		catalog:         catalogInstance,
		config:          cfg,
		currentDatabase: "default",
		currentUser:     "default",
	}, nil
}

// ValidateTable checks if a table exists in the catalog
func (ica *IceboxCatalogAdapter) ValidateTable(ctx context.Context, tableName string) error {
	if tableName == "" {
		return fmt.Errorf("table name cannot be empty")
	}

	if len(tableName) > 64 {
		return fmt.Errorf("table name too long: %s", tableName)
	}

	// Check if table exists in current database
	identifier := table.Identifier{ica.currentDatabase, tableName}
	exists, err := ica.catalog.CheckTableExists(ctx, identifier)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("table '%s' does not exist in database '%s'", tableName, ica.currentDatabase)
	}

	return nil
}

// ValidateColumn checks if a column exists in the specified table
func (ica *IceboxCatalogAdapter) ValidateColumn(ctx context.Context, tableName, columnName string) error {
	if columnName == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	if len(columnName) > 64 {
		return fmt.Errorf("column name too long: %s", columnName)
	}

	// First validate table exists
	if err := ica.ValidateTable(ctx, tableName); err != nil {
		return err
	}

	// TODO: Implement column validation when catalog supports schema retrieval
	// For now, we'll assume all columns exist (permissive mode)
	// This can be enhanced when we have access to table schemas
	return nil
}

// ValidateDatabase checks if a database exists
func (ica *IceboxCatalogAdapter) ValidateDatabase(ctx context.Context, dbName string) error {
	if dbName == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	// Check if namespace exists
	namespace := table.Identifier{dbName}
	exists, err := ica.catalog.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return fmt.Errorf("failed to check database existence: %w", err)
	}

	if !exists {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}

	return nil
}

// GetTableSchema retrieves the schema for a table
func (ica *IceboxCatalogAdapter) GetTableSchema(ctx context.Context, tableName string) (*TableSchema, error) {
	// Create table identifier
	identifier := table.Identifier{ica.currentDatabase, tableName}
	
	// Load the table from catalog
	tbl, err := ica.catalog.LoadTable(ctx, identifier, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to load table: %w", err)
	}

	// Convert Iceberg schema to our TableSchema
	return ica.convertIcebergSchemaToTableSchema(tbl.Schema()), nil
}

// GetColumnDefinition retrieves the definition for a specific column
func (ica *IceboxCatalogAdapter) GetColumnDefinition(ctx context.Context, tableName, columnName string) (*ColumnDefinition, error) {
	// TODO: Implement actual column definition retrieval when catalog supports it
	// For now, return nil to indicate no column info available
	return nil, nil
}

// ValidateDataType validates if a data type is supported
func (ica *IceboxCatalogAdapter) ValidateDataType(dataType string) error {
	// Convert to uppercase for case-insensitive comparison
	dataType = strings.ToUpper(dataType)

	// Define supported data types based on Iceberg types
	validTypes := map[string]bool{
		// Integer types
		"INT": true, "INTEGER": true, "BIGINT": true, "SMALLINT": true, "TINYINT": true,
		"LONG": true, "SHORT": true, "BYTE": true,

		// Floating point types
		"FLOAT": true, "DOUBLE": true, "REAL": true, "DECIMAL": true, "NUMERIC": true,

		// String types
		"STRING": true, "VARCHAR": true, "CHAR": true, "TEXT": true,

		// Boolean types
		"BOOLEAN": true, "BOOL": true,

		// Date/time types
		"DATE": true, "TIME": true, "TIMESTAMP": true, "DATETIME": true,

		// Binary types
		"BINARY": true, "VARBINARY": true, "BLOB": true,

		// Complex types
		"STRUCT": true, "LIST": true, "MAP": true, "JSON": true, "UUID": true,
	}

	if !validTypes[dataType] {
		return fmt.Errorf("unsupported data type: %s", dataType)
	}

	return nil
}

// IsCompatibleDataType checks if two data types are compatible
func (ica *IceboxCatalogAdapter) IsCompatibleDataType(sourceType, targetType string) bool {
	// Convert to uppercase for case-insensitive comparison
	sourceType = strings.ToUpper(sourceType)
	targetType = strings.ToUpper(targetType)

	// Exact match
	if sourceType == targetType {
		return true
	}

	// Type compatibility rules based on Iceberg type system
	compatibilityMap := map[string][]string{
		"INT":       {"INTEGER", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"INTEGER":   {"INT", "BIGINT", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"BIGINT":    {"INT", "INTEGER", "SMALLINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"SMALLINT":  {"INT", "INTEGER", "BIGINT", "TINYINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"TINYINT":   {"INT", "INTEGER", "BIGINT", "SMALLINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"FLOAT":     {"DOUBLE", "DECIMAL"},
		"DOUBLE":    {"FLOAT", "DECIMAL"},
		"DECIMAL":   {"FLOAT", "DOUBLE"},
		"STRING":    {"VARCHAR", "CHAR", "TEXT"},
		"VARCHAR":   {"STRING", "CHAR", "TEXT"},
		"CHAR":      {"STRING", "VARCHAR", "TEXT"},
		"TEXT":      {"STRING", "VARCHAR", "CHAR"},
		"BINARY":    {"VARBINARY", "BLOB"},
		"VARBINARY": {"BINARY", "BLOB"},
		"BLOB":      {"BINARY", "VARBINARY"},
	}

	if compatibleTypes, exists := compatibilityMap[sourceType]; exists {
		for _, compatibleType := range compatibleTypes {
			if compatibleType == targetType {
				return true
			}
		}
	}

	return false
}

// CheckPrivilege checks if the current user has the required privilege
func (ica *IceboxCatalogAdapter) CheckPrivilege(ctx context.Context, user, database, table, action string) error {
	// TODO: Implement actual privilege checking when security is implemented
	// For now, we'll assume all users have all privileges (permissive mode)
	return nil
}

// GetCurrentDatabase returns the current database context
func (ica *IceboxCatalogAdapter) GetCurrentDatabase(ctx context.Context) string {
	return ica.currentDatabase
}

// GetCurrentUser returns the current user context
func (ica *IceboxCatalogAdapter) GetCurrentUser(ctx context.Context) string {
	return ica.currentUser
}

// SetCurrentDatabase sets the current database context
func (ica *IceboxCatalogAdapter) SetCurrentDatabase(database string) {
	ica.currentDatabase = database
}

// SetCurrentUser sets the current user context
func (ica *IceboxCatalogAdapter) SetCurrentUser(user string) {
	ica.currentUser = user
}

// CreateTable creates a new table in the catalog
func (ica *IceboxCatalogAdapter) CreateTable(ctx context.Context, database, tableName string, schema *TableSchema) error {
	// Convert our TableSchema to Iceberg Schema
	icebergSchema, err := ica.convertTableSchemaToIceberg(schema)
	if err != nil {
		return fmt.Errorf("failed to convert schema: %w", err)
	}

	// Create table identifier
	identifier := table.Identifier{database, tableName}

	// Create the table using the catalog
	_, err = ica.catalog.CreateTable(ctx, identifier, icebergSchema)
	if err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	// Update current database context if needed
	if ica.currentDatabase == "default" {
		ica.currentDatabase = database
	}

	return nil
}

// DropTable drops a table from the catalog
func (ica *IceboxCatalogAdapter) DropTable(ctx context.Context, database, tableName string) error {
	identifier := table.Identifier{database, tableName}
	return ica.catalog.DropTable(ctx, identifier)
}

// AlterTable alters an existing table
func (ica *IceboxCatalogAdapter) AlterTable(ctx context.Context, database, tableName string, operations []AlterOperation) error {
	// TODO: Implement actual table alteration using catalog
	// This requires proper Iceberg table.Update types which are not yet available
	// For now, return an error indicating this is not yet implemented
	return fmt.Errorf("table alteration not yet implemented - requires Iceberg table.Update types")
}

// ListTables lists all tables in a database
func (ica *IceboxCatalogAdapter) ListTables(ctx context.Context, database string) ([]string, error) {
	namespace := table.Identifier{database}
	tables := ica.catalog.ListTables(ctx, namespace)

	var tableNames []string
	tables(func(identifier table.Identifier, err error) bool {
		if err != nil {
			return false // Stop iteration on error
		}
		tableNames = append(tableNames, identifier[len(identifier)-1]) // Get the table name (last element)
		return true                                                    // Continue iteration
	})

	return tableNames, nil
}

// ListColumns lists all columns in a table
func (ica *IceboxCatalogAdapter) ListColumns(ctx context.Context, database, table string) ([]ColumnInfo, error) {
	// TODO: Implement actual column listing when catalog supports schema retrieval
	// For now, return empty list
	return []ColumnInfo{}, nil
}

// BeginTransaction begins a new transaction
func (ica *IceboxCatalogAdapter) BeginTransaction(ctx context.Context) (TransactionID, error) {
	// TODO: Implement actual transaction management when catalog supports it
	// For now, return a dummy transaction ID
	return TransactionID("dummy"), nil
}

// CommitTransaction commits a transaction
func (ica *IceboxCatalogAdapter) CommitTransaction(ctx context.Context, txID TransactionID) error {
	// TODO: Implement actual transaction commit when catalog supports it
	return nil
}

// RollbackTransaction rolls back a transaction
func (ica *IceboxCatalogAdapter) RollbackTransaction(ctx context.Context, txID TransactionID) error {
	// TODO: Implement actual transaction rollback when catalog supports it
	return nil
}

// Close closes the catalog adapter
func (ica *IceboxCatalogAdapter) Close() error {
	return ica.catalog.Close()
}

// convertTableSchemaToIceberg converts our TableSchema to Iceberg Schema
func (ica *IceboxCatalogAdapter) convertTableSchemaToIceberg(tableSchema *TableSchema) (*iceberg.Schema, error) {
	if tableSchema == nil {
		return nil, fmt.Errorf("table schema cannot be nil")
	}

	// Create Iceberg fields from our column definitions
	fields := make([]iceberg.NestedField, 0, len(tableSchema.ColumnDefinitions))
	nextID := 1 // Start with ID 1

	for colName, colDef := range tableSchema.ColumnDefinitions {
		// Convert our data type to Iceberg type
		icebergType, err := ica.convertDataTypeToIceberg(
			colDef.DataType,
			colDef.Length,
			colDef.Precision,
			colDef.Scale,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to convert column '%s' type: %w", colName, err)
		}

		// Create Iceberg field
		field := iceberg.NestedField{
			ID:       nextID,
			Name:     colName,
			Type:     icebergType,
			Required: !colDef.Nullable,
			Doc:      "", // TODO: Add support for column documentation
		}

		fields = append(fields, field)
		nextID++
	}

	// Create Iceberg Schema with schema ID 0
	schema := iceberg.NewSchema(0, fields...)
	return schema, nil
}

// convertDataTypeToIceberg converts our data type to Iceberg type
func (ica *IceboxCatalogAdapter) convertDataTypeToIceberg(dataType string, length, precision, scale int) (iceberg.Type, error) {
	// Convert to uppercase for case-insensitive comparison
	dataType = strings.ToUpper(dataType)

	switch dataType {
	case "INT", "INTEGER":
		return iceberg.PrimitiveTypes.Int32, nil
	case "BIGINT", "LONG":
		return iceberg.PrimitiveTypes.Int64, nil
	case "SMALLINT", "SHORT":
		return iceberg.PrimitiveTypes.Int32, nil
	case "TINYINT":
		return iceberg.PrimitiveTypes.Int32, nil
	case "FLOAT":
		return iceberg.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return iceberg.PrimitiveTypes.Float64, nil
	case "DECIMAL", "NUMERIC":
		if precision <= 0 {
			precision = 38 // Default precision
		}
		if scale < 0 {
			scale = 0 // Default scale
		}
		return iceberg.DecimalTypeOf(precision, scale), nil
	case "STRING", "VARCHAR", "CHAR", "TEXT":
		if length > 0 {
			// For fixed-length strings, we could use FixedType
			// But for now, we'll use String type
		}
		return iceberg.PrimitiveTypes.String, nil
	case "BOOLEAN", "BOOL":
		return iceberg.PrimitiveTypes.Bool, nil
	case "DATE":
		return iceberg.PrimitiveTypes.Date, nil
	case "TIME":
		return iceberg.PrimitiveTypes.Time, nil
	case "TIMESTAMP", "DATETIME":
		return iceberg.PrimitiveTypes.Timestamp, nil
	case "BINARY", "VARBINARY", "BLOB":
		if length > 0 {
			return iceberg.FixedTypeOf(length), nil
		}
		return iceberg.PrimitiveTypes.Binary, nil
	case "UUID":
		return iceberg.PrimitiveTypes.UUID, nil
	case "JSON":
		// JSON is represented as a string in Iceberg
		return iceberg.PrimitiveTypes.String, nil
	default:
		return nil, fmt.Errorf("unsupported data type: %s", dataType)
	}
}

// convertIcebergSchemaToTableSchema converts Iceberg Schema to our TableSchema
func (ica *IceboxCatalogAdapter) convertIcebergSchemaToTableSchema(icebergSchema *iceberg.Schema) *TableSchema {
	tableSchema := &TableSchema{
		ColumnDefinitions: make(map[string]*ColumnDefinition),
	}

	// Convert each field
	for _, field := range icebergSchema.Fields() {
		colDef := &ColumnDefinition{
			DataType:   ica.convertIcebergTypeToDataType(field.Type),
			Length:     0, // TODO: Extract from Iceberg type if available
			Precision:  0, // TODO: Extract from Iceberg type if available
			Scale:      0, // TODO: Extract from Iceberg type if available
			Nullable:   !field.Required,
			Default:    nil, // TODO: Extract from Iceberg field if available
			References: nil, // TODO: Extract from Iceberg field if available
			Sequence:   nil, // TODO: Extract from Iceberg field if available
			NotNull:    field.Required,
			Unique:     false, // TODO: Extract from Iceberg field if available
			Check:      nil, // TODO: Extract from Iceberg field if available
		}

		tableSchema.ColumnDefinitions[field.Name] = colDef
	}

	return tableSchema
}

// convertIcebergTypeToDataType converts Iceberg type to our data type string
func (ica *IceboxCatalogAdapter) convertIcebergTypeToDataType(icebergType iceberg.Type) string {
	switch t := icebergType.(type) {
	case *iceberg.DecimalType:
		return "DECIMAL"
	case *iceberg.FixedType:
		return "BINARY"
	case iceberg.PrimitiveType:
		switch t {
		case iceberg.PrimitiveTypes.Bool:
			return "BOOLEAN"
		case iceberg.PrimitiveTypes.Int32:
			return "INT"
		case iceberg.PrimitiveTypes.Int64:
			return "BIGINT"
		case iceberg.PrimitiveTypes.Float32:
			return "FLOAT"
		case iceberg.PrimitiveTypes.Float64:
			return "DOUBLE"
		case iceberg.PrimitiveTypes.String:
			return "STRING"
		case iceberg.PrimitiveTypes.Date:
			return "DATE"
		case iceberg.PrimitiveTypes.Time:
			return "TIME"
		case iceberg.PrimitiveTypes.Timestamp:
			return "TIMESTAMP"
		case iceberg.PrimitiveTypes.TimestampTz:
			return "TIMESTAMP"
		case iceberg.PrimitiveTypes.Binary:
			return "BINARY"
		case iceberg.PrimitiveTypes.UUID:
			return "UUID"
		default:
			return "STRING"
		}
	case *iceberg.ListType:
		return "LIST"
	case *iceberg.MapType:
		return "MAP"
	case *iceberg.StructType:
		return "STRUCT"
	default:
		return "STRING"
	}
}
