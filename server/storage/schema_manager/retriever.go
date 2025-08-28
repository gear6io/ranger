package schema_manager

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/uptrace/bun"
)

// SchemaRetriever handles retrieving and reconstructing schemas from TableColumn records
type SchemaRetriever struct {
	metadataManager MetadataManagerInterface
}

// MetadataManagerInterface defines the minimal interface we need from MetadataManager
type MetadataManagerInterface interface {
	GetBunDB() *bun.DB
}

// NewSchemaRetriever creates a new schema retriever
func NewSchemaRetriever(metadataManager MetadataManagerInterface) *SchemaRetriever {
	return &SchemaRetriever{
		metadataManager: metadataManager,
	}
}

// RetrieveSchema loads TableColumn records and constructs an Iceberg schema
// Requirement 4.4, 4.5: Enhanced error reporting for schema retrieval and parsing failures
func (sr *SchemaRetriever) RetrieveSchema(ctx context.Context, database, tableName string) (*iceberg.Schema, error) {
	// Get table ID by querying the tables table directly
	tableID, err := sr.getTableID(ctx, database, tableName)
	if err != nil {
		// Create detailed schema retrieval error (Requirement 4.4)
		retrievalErr := NewSchemaRetrievalError(database, tableName, "database_error", err)
		return nil, errors.New(SchemaManagerRetrievalError, retrievalErr.Message, retrievalErr).
			AddContext("database", database).
			AddContext("table", tableName).
			AddContext("error_type", "table_lookup_failed")
	}

	// Get table columns from the registry
	columns, err := sr.getTableColumns(ctx, tableID)
	if err != nil {
		// Create detailed schema retrieval error (Requirement 4.4)
		retrievalErr := NewSchemaRetrievalError(database, tableName, "database_error", err)
		return nil, errors.New(SchemaManagerRetrievalError, retrievalErr.Message, retrievalErr).
			AddContext("database", database).
			AddContext("table", tableName).
			AddContext("table_id", fmt.Sprintf("%d", tableID)).
			AddContext("error_type", "column_retrieval_failed")
	}

	if len(columns) == 0 {
		return nil, errors.New(SchemaManagerNotFound, "table has no columns", nil).
			AddContext("database", database).
			AddContext("table", tableName).
			AddContext("error_type", "empty_schema")
	}

	// Sort columns by ordinal position
	sort.Slice(columns, func(i, j int) bool {
		return columns[i].OrdinalPosition < columns[j].OrdinalPosition
	})

	// Convert columns to Iceberg fields
	fields := make([]iceberg.NestedField, 0, len(columns))
	for i, column := range columns {
		field, err := sr.convertColumnToIcebergField(column, i)
		if err != nil {
			// Create detailed schema parsing error (Requirement 4.5)
			parsingErr := NewSchemaParsingError(database, tableName, column.ColumnName, column.DataType, err)
			return nil, errors.New(SchemaManagerParsingError, parsingErr.Message, parsingErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("column", column.ColumnName).
				AddContext("data_type", column.DataType).
				AddContext("error_type", "column_parsing_failed")
		}
		fields = append(fields, field)
	}

	// Create Iceberg schema
	schema := iceberg.NewSchema(0, fields...)
	return schema, nil
}

// getTableID retrieves the table ID for a given database and table name
func (sr *SchemaRetriever) getTableID(ctx context.Context, database, tableName string) (int64, error) {
	// Get the bun database connection
	db := sr.metadataManager.GetBunDB()
	if db == nil {
		return 0, errors.New(SchemaManagerRetrievalError, "database connection not available", nil)
	}

	// First get the database ID
	var dbRecord regtypes.Database
	err := db.NewSelect().
		Model(&dbRecord).
		Where("name = ?", database).
		Scan(ctx)
	if err != nil {
		return 0, errors.New(SchemaManagerRetrievalError, "failed to find database", err).
			AddContext("database", database)
	}

	// Then get the table ID
	var tableRecord regtypes.Table
	err = db.NewSelect().
		Model(&tableRecord).
		Where("database_id = ? AND name = ?", dbRecord.ID, tableName).
		Scan(ctx)
	if err != nil {
		return 0, errors.New(SchemaManagerRetrievalError, "failed to find table", err).
			AddContext("database", database).
			AddContext("table", tableName)
	}

	return tableRecord.ID, nil
}

// getTableColumns retrieves table columns from the database
func (sr *SchemaRetriever) getTableColumns(ctx context.Context, tableID int64) ([]*regtypes.TableColumn, error) {
	// Get the bun database connection
	db := sr.metadataManager.GetBunDB()
	if db == nil {
		return nil, errors.New(SchemaManagerRetrievalError, "database connection not available", nil)
	}

	var columns []*regtypes.TableColumn
	err := db.NewSelect().
		Model(&columns).
		Where("table_id = ?", tableID).
		Order("ordinal_position ASC").
		Scan(ctx)

	if err != nil {
		return nil, errors.New(SchemaManagerRetrievalError, "failed to query table columns", err).
			AddContext("table_id", fmt.Sprintf("%d", tableID))
	}

	return columns, nil
}

// convertColumnToIcebergField converts a TableColumn to an Iceberg NestedField
func (sr *SchemaRetriever) convertColumnToIcebergField(column *regtypes.TableColumn, fieldID int) (iceberg.NestedField, error) {
	// Parse the Iceberg type from the DataType string
	icebergType, err := sr.parseIcebergType(column.DataType)
	if err != nil {
		return iceberg.NestedField{}, errors.New(SchemaManagerParsingError, "failed to parse Iceberg type", err).
			AddContext("column", column.ColumnName).
			AddContext("data_type", column.DataType)
	}

	// Create the Iceberg field
	field := iceberg.NestedField{
		ID:       fieldID,
		Name:     column.ColumnName,
		Type:     icebergType,
		Required: !column.IsNullable,
	}

	return field, nil
}

// parseIcebergType parses an Iceberg type string into an iceberg.Type
func (sr *SchemaRetriever) parseIcebergType(dataType string) (iceberg.Type, error) {
	dataType = strings.TrimSpace(dataType)

	// Handle primitive types
	switch dataType {
	case "boolean":
		return iceberg.PrimitiveTypes.Bool, nil
	case "int32":
		return iceberg.PrimitiveTypes.Int32, nil
	case "int64":
		return iceberg.PrimitiveTypes.Int64, nil
	case "float32":
		return iceberg.PrimitiveTypes.Float32, nil
	case "float64":
		return iceberg.PrimitiveTypes.Float64, nil
	case "string":
		return iceberg.PrimitiveTypes.String, nil
	case "binary":
		return iceberg.PrimitiveTypes.Binary, nil
	case "date":
		return iceberg.PrimitiveTypes.Date, nil
	case "time":
		return iceberg.PrimitiveTypes.Time, nil
	case "timestamp":
		return iceberg.PrimitiveTypes.Timestamp, nil
	case "timestamptz":
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case "uuid":
		return iceberg.PrimitiveTypes.UUID, nil
	}

	// Handle complex types
	if strings.HasPrefix(dataType, "list<") && strings.HasSuffix(dataType, ">") {
		return sr.parseListType(dataType)
	}

	if strings.HasPrefix(dataType, "map<") && strings.HasSuffix(dataType, ">") {
		return sr.parseMapType(dataType)
	}

	if strings.HasPrefix(dataType, "struct<") && strings.HasSuffix(dataType, ">") {
		return sr.parseStructType(dataType)
	}

	// Handle decimal types
	if strings.HasPrefix(dataType, "decimal(") && strings.HasSuffix(dataType, ")") {
		return sr.parseDecimalType(dataType)
	}

	// Handle fixed types
	if strings.HasPrefix(dataType, "fixed(") && strings.HasSuffix(dataType, ")") {
		return sr.parseFixedType(dataType)
	}

	return nil, errors.New(SchemaManagerParsingError, "unsupported data type", nil).
		AddContext("data_type", dataType)
}

// parseListType parses a list type string like "list<int32>"
func (sr *SchemaRetriever) parseListType(dataType string) (iceberg.Type, error) {
	// Extract element type from "list<element_type>"
	elementTypeStr := dataType[5 : len(dataType)-1] // Remove "list<" and ">"
	elementType, err := sr.parseIcebergType(elementTypeStr)
	if err != nil {
		return nil, errors.New(SchemaManagerParsingError, "failed to parse list element type", err).
			AddContext("element_type", elementTypeStr)
	}

	return &iceberg.ListType{Element: elementType}, nil
}

// parseMapType parses a map type string like "map<string,int64>"
func (sr *SchemaRetriever) parseMapType(dataType string) (iceberg.Type, error) {
	// Extract key and value types from "map<key_type,value_type>"
	content := dataType[4 : len(dataType)-1] // Remove "map<" and ">"

	// Find the comma that separates key and value types
	// This is simplified - a full parser would handle nested types properly
	commaIndex := strings.Index(content, ",")
	if commaIndex == -1 {
		return nil, errors.New(SchemaManagerParsingError, "invalid map type format", nil).
			AddContext("data_type", dataType)
	}

	keyTypeStr := strings.TrimSpace(content[:commaIndex])
	valueTypeStr := strings.TrimSpace(content[commaIndex+1:])

	keyType, err := sr.parseIcebergType(keyTypeStr)
	if err != nil {
		return nil, errors.New(SchemaManagerParsingError, "failed to parse map key type", err).
			AddContext("key_type", keyTypeStr)
	}

	valueType, err := sr.parseIcebergType(valueTypeStr)
	if err != nil {
		return nil, errors.New(SchemaManagerParsingError, "failed to parse map value type", err).
			AddContext("value_type", valueTypeStr)
	}

	return &iceberg.MapType{KeyType: keyType, ValueType: valueType}, nil
}

// parseStructType parses a struct type string like "struct<field1:string,field2:int32>"
func (sr *SchemaRetriever) parseStructType(dataType string) (iceberg.Type, error) {
	// Extract field definitions from "struct<field1:type1,field2:type2>"
	content := dataType[7 : len(dataType)-1] // Remove "struct<" and ">"

	if content == "" {
		return &iceberg.StructType{}, nil // Empty struct
	}

	// Split by comma to get individual field definitions
	// This is simplified - a full parser would handle nested types properly
	fieldDefs := strings.Split(content, ",")
	fields := make([]iceberg.NestedField, 0, len(fieldDefs))

	for i, fieldDef := range fieldDefs {
		fieldDef = strings.TrimSpace(fieldDef)

		// Split by colon to get field name and type
		colonIndex := strings.Index(fieldDef, ":")
		if colonIndex == -1 {
			return nil, errors.New(SchemaManagerParsingError, "invalid struct field format", nil).
				AddContext("field_def", fieldDef)
		}

		fieldName := strings.TrimSpace(fieldDef[:colonIndex])
		fieldTypeStr := strings.TrimSpace(fieldDef[colonIndex+1:])

		if fieldName == "" {
			return nil, errors.New(SchemaManagerParsingError, "struct field name cannot be empty", nil).
				AddContext("field_def", fieldDef)
		}

		if fieldTypeStr == "" {
			return nil, errors.New(SchemaManagerParsingError, "struct field type cannot be empty", nil).
				AddContext("field_name", fieldName)
		}

		fieldType, err := sr.parseIcebergType(fieldTypeStr)
		if err != nil {
			return nil, errors.New(SchemaManagerParsingError, "failed to parse struct field type", err).
				AddContext("field_name", fieldName).
				AddContext("field_type", fieldTypeStr)
		}

		field := iceberg.NestedField{
			ID:       i,
			Name:     fieldName,
			Type:     fieldType,
			Required: true, // Default to required for struct fields
		}

		fields = append(fields, field)
	}

	return &iceberg.StructType{FieldList: fields}, nil
}

// parseDecimalType parses a decimal type string like "decimal(10,2)"
func (sr *SchemaRetriever) parseDecimalType(dataType string) (iceberg.Type, error) {
	// Extract precision and scale from "decimal(precision,scale)"
	content := dataType[8 : len(dataType)-1] // Remove "decimal(" and ")"

	parts := strings.Split(content, ",")
	if len(parts) != 2 {
		return nil, errors.New(SchemaManagerParsingError, "invalid decimal format", nil).
			AddContext("data_type", dataType)
	}

	precision, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return nil, errors.New(SchemaManagerParsingError, "invalid decimal precision", err).
			AddContext("precision", parts[0])
	}

	scale, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return nil, errors.New(SchemaManagerParsingError, "invalid decimal scale", err).
			AddContext("scale", parts[1])
	}

	return iceberg.DecimalTypeOf(precision, scale), nil
}

// parseFixedType parses a fixed type string like "fixed(16)"
func (sr *SchemaRetriever) parseFixedType(dataType string) (iceberg.Type, error) {
	// Extract length from "fixed(length)"
	content := dataType[6 : len(dataType)-1] // Remove "fixed(" and ")"

	length, err := strconv.Atoi(strings.TrimSpace(content))
	if err != nil {
		return nil, errors.New(SchemaManagerParsingError, "invalid fixed length", err).
			AddContext("length", content)
	}

	return iceberg.FixedTypeOf(length), nil
}
