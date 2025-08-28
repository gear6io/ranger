package schema

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/pkg/errors"
)

// Package-specific error codes for parquet schema
var (
	ParquetSchemaNilSchema              = errors.MustNewCode("parquet.schema_nil_schema")
	ParquetSchemaFieldConversionFailed  = errors.MustNewCode("parquet.schema_field_conversion_failed")
	ParquetSchemaTypeConversionFailed   = errors.MustNewCode("parquet.schema_type_conversion_failed")
	ParquetSchemaUnsupportedType        = errors.MustNewCode("parquet.schema_unsupported_type")
	ParquetSchemaListConversionFailed   = errors.MustNewCode("parquet.schema_list_conversion_failed")
	ParquetSchemaMapConversionFailed    = errors.MustNewCode("parquet.schema_map_conversion_failed")
	ParquetSchemaStructConversionFailed = errors.MustNewCode("parquet.schema_struct_conversion_failed")
	ParquetSchemaValidationFailed       = errors.MustNewCode("parquet.schema_validation_failed")
	ParquetSchemaColumnMismatch         = errors.MustNewCode("parquet.schema_column_mismatch")
	ParquetSchemaRowValidationFailed    = errors.MustNewCode("parquet.schema_row_validation_failed")
	ParquetSchemaFieldCannotBeNull      = errors.MustNewCode("parquet.schema_field_cannot_be_null")
	ParquetSchemaTypeMismatch           = errors.MustNewCode("parquet.schema_type_mismatch")
)

// DetailedValidationError represents a detailed validation error with context
// This implements Requirement 4.1, 4.2, 4.3 for detailed error information
type DetailedValidationError struct {
	RowIndex     int         `json:"row_index"`
	ColumnIndex  int         `json:"column_index"`
	ColumnName   string      `json:"column_name"`
	ExpectedType string      `json:"expected_type"`
	ActualType   string      `json:"actual_type"`
	Value        interface{} `json:"value"`
	Message      string      `json:"message"`
	TableName    string      `json:"table_name"`
	Database     string      `json:"database"`
}

// Error implements the error interface
func (ve DetailedValidationError) Error() string {
	return ve.Message
}

// NewDetailedValidationError creates a new validation error with comprehensive context
// Requirement 4.1, 4.2, 4.3: Include specific validation failure reason, row/column info, and type information
func NewDetailedValidationError(rowIndex, columnIndex int, columnName, expectedType, actualType string, value interface{}, database, tableName string) *DetailedValidationError {
	message := fmt.Sprintf("validation failed at row %d, column %d (%s): expected %s but got %s (value: %v)",
		rowIndex, columnIndex, columnName, expectedType, actualType, value)

	return &DetailedValidationError{
		RowIndex:     rowIndex,
		ColumnIndex:  columnIndex,
		ColumnName:   columnName,
		ExpectedType: expectedType,
		ActualType:   actualType,
		Value:        value,
		Message:      message,
		TableName:    tableName,
		Database:     database,
	}
}

// NewColumnCountValidationError creates a validation error for column count mismatches
// Requirement 4.1, 4.2: Include specific validation failure reason and row information
func NewColumnCountValidationError(rowIndex, actualColumns, expectedColumns int, database, tableName string) *DetailedValidationError {
	message := fmt.Sprintf("validation failed at row %d: expected %d columns but got %d columns",
		rowIndex, expectedColumns, actualColumns)

	return &DetailedValidationError{
		RowIndex:     rowIndex,
		ColumnIndex:  -1, // Not applicable for column count errors
		ColumnName:   "",
		ExpectedType: fmt.Sprintf("%d columns", expectedColumns),
		ActualType:   fmt.Sprintf("%d columns", actualColumns),
		Value:        actualColumns,
		Message:      message,
		TableName:    tableName,
		Database:     database,
	}
}

// NewNullValueValidationError creates a validation error for null value violations
// Requirement 4.1, 4.2, 4.3: Include specific validation failure reason, row/column info, and field requirements
func NewNullValueValidationError(rowIndex, columnIndex int, columnName, database, tableName string) *DetailedValidationError {
	message := fmt.Sprintf("validation failed at row %d, column %d (%s): field cannot be null",
		rowIndex, columnIndex, columnName)

	return &DetailedValidationError{
		RowIndex:     rowIndex,
		ColumnIndex:  columnIndex,
		ColumnName:   columnName,
		ExpectedType: "non-null value",
		ActualType:   "null",
		Value:        nil,
		Message:      message,
		TableName:    tableName,
		Database:     database,
	}
}

// Manager handles schema conversion and validation
type Manager struct {
	config *ParquetConfig
}

// NewManager creates a new schema manager
func NewManager(config *ParquetConfig) *Manager {
	return &Manager{
		config: config,
	}
}

// ConvertIcebergToArrowSchema converts an Iceberg schema to an Arrow schema
func (sm *Manager) ConvertIcebergToArrowSchema(schema *iceberg.Schema) (*arrow.Schema, error) {
	if schema == nil {
		return nil, errors.New(ParquetSchemaNilSchema, "iceberg schema cannot be nil", nil)
	}

	fields := make([]arrow.Field, 0, len(schema.Fields()))

	for _, field := range schema.Fields() {
		arrowField, err := sm.convertIcebergField(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrowField)
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertIcebergField converts a single Iceberg field to an Arrow field
func (sm *Manager) convertIcebergField(field iceberg.NestedField) (arrow.Field, error) {
	arrowType, err := sm.convertIcebergType(field.Type)
	if err != nil {
		return arrow.Field{}, err
	}

	return arrow.Field{
		Name:     field.Name,
		Type:     arrowType,
		Nullable: !field.Required,
		Metadata: arrow.MetadataFrom(map[string]string{
			"iceberg_id":       fmt.Sprintf("%d", field.ID),
			"iceberg_required": fmt.Sprintf("%t", field.Required),
		}),
	}, nil
}

// convertIcebergType converts an Iceberg type to an Arrow type
func (sm *Manager) convertIcebergType(icebergType iceberg.Type) (arrow.DataType, error) {
	switch icebergType {
	case iceberg.PrimitiveTypes.Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case iceberg.PrimitiveTypes.Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case iceberg.PrimitiveTypes.Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case iceberg.PrimitiveTypes.Float32:
		return arrow.PrimitiveTypes.Float32, nil
	case iceberg.PrimitiveTypes.Float64:
		return arrow.PrimitiveTypes.Float64, nil
	case iceberg.PrimitiveTypes.String:
		return arrow.BinaryTypes.String, nil
	case iceberg.PrimitiveTypes.Binary:
		return arrow.BinaryTypes.Binary, nil
	case iceberg.PrimitiveTypes.Date:
		return arrow.FixedWidthTypes.Date32, nil
	case iceberg.PrimitiveTypes.Time:
		return arrow.FixedWidthTypes.Time64ns, nil
	case iceberg.PrimitiveTypes.Timestamp:
		return arrow.FixedWidthTypes.Timestamp_ns, nil
	case iceberg.PrimitiveTypes.TimestampTz:
		return arrow.FixedWidthTypes.Timestamp_ns, nil
	case iceberg.PrimitiveTypes.UUID:
		return arrow.BinaryTypes.String, nil // Store UUID as string for now
	default:
		// Handle complex types
		switch t := icebergType.(type) {
		case *iceberg.ListType:
			return sm.convertListType(t)
		case *iceberg.MapType:
			return sm.convertMapType(t)
		case *iceberg.StructType:
			return sm.convertStructType(t)
		default:
			return nil, errors.New(ParquetSchemaUnsupportedType, "unsupported iceberg type", nil).AddContext("type", fmt.Sprintf("%T", icebergType))
		}
	}
}

// convertListType converts Iceberg list type to Arrow list type
func (sm *Manager) convertListType(lt *iceberg.ListType) (arrow.DataType, error) {
	elementType, err := sm.convertIcebergType(lt.Element)
	if err != nil {
		return nil, err
	}

	return arrow.ListOf(elementType), nil
}

// convertMapType converts Iceberg map type to Arrow map type
func (sm *Manager) convertMapType(mt *iceberg.MapType) (arrow.DataType, error) {
	keyType, err := sm.convertIcebergType(mt.KeyType)
	if err != nil {
		return nil, err
	}

	valueType, err := sm.convertIcebergType(mt.ValueType)
	if err != nil {
		return nil, err
	}

	return arrow.MapOf(keyType, valueType), nil
}

// convertStructType converts Iceberg struct type to Arrow struct type
func (sm *Manager) convertStructType(st *iceberg.StructType) (arrow.DataType, error) {
	fields := make([]arrow.Field, 0, len(st.Fields()))

	for _, field := range st.Fields() {
		arrowField, err := sm.convertIcebergField(field)
		if err != nil {
			return nil, err
		}
		fields = append(fields, arrowField)
	}

	return arrow.StructOf(fields...), nil
}

// ValidateData validates data against a given schema
// Requirement 3.4: Return immediately on first validation failure (fail-fast)
// Requirement 4.1, 4.2, 4.3: Return detailed error information including row and column details
func (sm *Manager) ValidateData(data [][]interface{}, schema *arrow.Schema) error {
	if len(data) == 0 {
		return nil // Empty data is valid
	}

	if schema == nil {
		return errors.New(ParquetSchemaNilSchema, "schema cannot be nil", nil)
	}

	expectedColumns := len(schema.Fields())

	for rowIndex, row := range data {
		if len(row) != expectedColumns {
			return errors.New(ParquetSchemaColumnMismatch, "row has incorrect number of columns", nil).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("actual_columns", fmt.Sprintf("%d", len(row))).
				AddContext("expected_columns", fmt.Sprintf("%d", expectedColumns)).
				AddContext("validation_type", "column_count_mismatch")
		}

		// Validate each field in the row - fail fast on first error (Requirement 3.4)
		if err := sm.validateRow(row, schema, rowIndex); err != nil {
			return err
		}
	}

	return nil
}

// ValidateDataWithContext validates data against a schema with table context for enhanced error reporting
// Requirement 4.1, 4.2, 4.3: Include database and table context in validation errors
func (sm *Manager) ValidateDataWithContext(data [][]interface{}, schema *arrow.Schema, database, tableName string) error {
	if len(data) == 0 {
		return nil // Empty data is valid
	}

	if schema == nil {
		return errors.New(ParquetSchemaNilSchema, "schema cannot be nil", nil).
			AddContext("database", database).
			AddContext("table", tableName)
	}

	expectedColumns := len(schema.Fields())

	for rowIndex, row := range data {
		if len(row) != expectedColumns {
			// Create detailed column count validation error
			validationErr := NewColumnCountValidationError(rowIndex, len(row), expectedColumns, database, tableName)
			return errors.New(ParquetSchemaColumnMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "column_count_mismatch")
		}

		// Validate each field in the row - fail fast on first error (Requirement 3.4)
		if err := sm.validateRowWithContext(row, schema, rowIndex, database, tableName); err != nil {
			return err
		}
	}

	return nil
}

// validateRow validates a single row against the schema
func (sm *Manager) validateRow(row []interface{}, schema *arrow.Schema, rowIndex int) error {
	for colIndex, value := range row {
		field := schema.Field(colIndex)

		if err := sm.validateValue(value, field, rowIndex, colIndex); err != nil {
			return err
		}
	}

	return nil
}

// validateRowWithContext validates a single row against the schema with table context
// Requirement 4.1, 4.2, 4.3: Include database and table context in validation errors
func (sm *Manager) validateRowWithContext(row []interface{}, schema *arrow.Schema, rowIndex int, database, tableName string) error {
	for colIndex, value := range row {
		field := schema.Field(colIndex)

		if err := sm.validateValueWithContext(value, field, rowIndex, colIndex, database, tableName); err != nil {
			return err
		}
	}

	return nil
}

// validateValue validates a single value against a field
func (sm *Manager) validateValue(value interface{}, field arrow.Field, rowIndex, colIndex int) error {
	// Handle null values
	if value == nil {
		if !field.Nullable {
			return errors.New(ParquetSchemaFieldCannotBeNull, "field cannot be null", nil).
				AddContext("field_name", field.Name).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
		return nil
	}

	// Validate type compatibility
	if err := sm.validateType(value, field.Type, field.Name, rowIndex, colIndex); err != nil {
		return err
	}

	return nil
}

// validateValueWithContext validates a single value against a field with table context
// Requirement 4.1, 4.2, 4.3: Include detailed validation context including database and table
func (sm *Manager) validateValueWithContext(value interface{}, field arrow.Field, rowIndex, colIndex int, database, tableName string) error {
	// Handle null values
	if value == nil {
		if !field.Nullable {
			// Create detailed null value validation error
			validationErr := NewNullValueValidationError(rowIndex, colIndex, field.Name, database, tableName)
			return errors.New(ParquetSchemaFieldCannotBeNull, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "null_constraint_violation")
		}
		return nil
	}

	// Validate type compatibility with enhanced error context
	if err := sm.validateTypeWithContext(value, field.Type, field.Name, rowIndex, colIndex, database, tableName); err != nil {
		return err
	}

	return nil
}

// validateType validates that a value is compatible with an Arrow type
func (sm *Manager) validateType(value interface{}, arrowType arrow.DataType, fieldName string, rowIndex, colIndex int) error {
	switch arrowType.(type) {
	case *arrow.BooleanType:
		if _, ok := value.(bool); !ok {
			return errors.New(ParquetSchemaTypeMismatch, "field expects boolean", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.Int32Type:
		if !sm.isInt32Compatible(value) {
			return errors.New(ParquetSchemaTypeMismatch, "field expects int32", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.Int64Type:
		if !sm.isInt64Compatible(value) {
			return errors.New(ParquetSchemaTypeMismatch, "field expects int64", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.Float32Type:
		if !sm.isFloat32Compatible(value) {
			return errors.New(ParquetSchemaTypeMismatch, "field expects float32", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.Float64Type:
		if !sm.isFloat64Compatible(value) {
			return errors.New(ParquetSchemaTypeMismatch, "field expects float64", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.StringType:
		if _, ok := value.(string); !ok {
			return errors.New(ParquetSchemaTypeMismatch, "field expects string", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.BinaryType:
		if _, ok := value.([]byte); !ok {
			return errors.New(ParquetSchemaTypeMismatch, "field expects []byte", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.Date32Type:
		if !sm.isDateCompatible(value) {
			return errors.New(ParquetSchemaTypeMismatch, "field expects date", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	case *arrow.TimestampType:
		if !sm.isTimestampCompatible(value) {
			return errors.New(ParquetSchemaTypeMismatch, "field expects timestamp", nil).
				AddContext("field_name", fieldName).
				AddContext("actual_type", fmt.Sprintf("%T", value)).
				AddContext("row_index", fmt.Sprintf("%d", rowIndex)).
				AddContext("col_index", fmt.Sprintf("%d", colIndex))
		}
	default:
		// For complex types, we'll do basic validation
		return nil
	}

	return nil
}

// validateTypeWithContext validates that a value is compatible with an Arrow type with enhanced error context
// Requirement 4.1, 4.2, 4.3: Include detailed validation context with expected vs actual types
func (sm *Manager) validateTypeWithContext(value interface{}, arrowType arrow.DataType, fieldName string, rowIndex, colIndex int, database, tableName string) error {
	actualType := fmt.Sprintf("%T", value)

	switch arrowType.(type) {
	case *arrow.BooleanType:
		if _, ok := value.(bool); !ok {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "boolean", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.Int32Type:
		if !sm.isInt32Compatible(value) {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "int32", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.Int64Type:
		if !sm.isInt64Compatible(value) {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "int64", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.Float32Type:
		if !sm.isFloat32Compatible(value) {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "float32", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.Float64Type:
		if !sm.isFloat64Compatible(value) {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "float64", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.StringType:
		if _, ok := value.(string); !ok {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "string", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.BinaryType:
		if _, ok := value.([]byte); !ok {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "[]byte", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.Date32Type:
		if !sm.isDateCompatible(value) {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "date", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	case *arrow.TimestampType:
		if !sm.isTimestampCompatible(value) {
			validationErr := NewDetailedValidationError(rowIndex, colIndex, fieldName, "timestamp", actualType, value, database, tableName)
			return errors.New(ParquetSchemaTypeMismatch, validationErr.Message, validationErr).
				AddContext("database", database).
				AddContext("table", tableName).
				AddContext("validation_type", "type_mismatch")
		}
	default:
		// For complex types, we'll do basic validation
		return nil
	}

	return nil
}

// Type compatibility helpers
func (sm *Manager) isInt32Compatible(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		// Check if float can be safely converted to int32
		return true // TODO: Add range checking
	default:
		return false
	}
}

func (sm *Manager) isInt64Compatible(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return true
	case uint, uint8, uint16, uint32, uint64:
		return true
	case float32, float64:
		return true // TODO: Add range checking
	default:
		return false
	}
}

func (sm *Manager) isFloat32Compatible(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func (sm *Manager) isFloat64Compatible(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func (sm *Manager) isDateCompatible(value interface{}) bool {
	switch value.(type) {
	case time.Time, string:
		return true
	default:
		return false
	}
}

func (sm *Manager) isTimestampCompatible(value interface{}) bool {
	switch value.(type) {
	case time.Time, string:
		return true
	default:
		return false
	}
}
