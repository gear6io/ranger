package parquet

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
)

// SchemaManager handles schema conversion and validation
type SchemaManager struct {
	config *ParquetConfig
}

// NewSchemaManager creates a new schema manager
func NewSchemaManager(config *ParquetConfig) *SchemaManager {
	return &SchemaManager{
		config: config,
	}
}

// ConvertIcebergToArrowSchema converts an Iceberg schema to an Arrow schema
func (sm *SchemaManager) ConvertIcebergToArrowSchema(schema *iceberg.Schema) (*arrow.Schema, error) {
	if schema == nil {
		return nil, fmt.Errorf("iceberg schema cannot be nil")
	}

	fields := make([]arrow.Field, 0, len(schema.Fields()))
	
	for _, field := range schema.Fields() {
		arrowField, err := sm.convertIcebergField(field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s: %w", field.Name, err)
		}
		fields = append(fields, arrowField)
	}

	return arrow.NewSchema(fields, nil), nil
}

// convertIcebergField converts a single Iceberg field to an Arrow field
func (sm *SchemaManager) convertIcebergField(field iceberg.NestedField) (arrow.Field, error) {
	arrowType, err := sm.convertIcebergType(field.Type)
	if err != nil {
		return arrow.Field{}, fmt.Errorf("failed to convert type for field %s: %w", field.Name, err)
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
func (sm *SchemaManager) convertIcebergType(icebergType iceberg.Type) (arrow.DataType, error) {
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
			return nil, fmt.Errorf("unsupported iceberg type: %T", icebergType)
		}
	}
}

// convertListType converts Iceberg list type to Arrow list type
func (sm *SchemaManager) convertListType(lt *iceberg.ListType) (arrow.DataType, error) {
	elementType, err := sm.convertIcebergType(lt.Element)
	if err != nil {
		return nil, fmt.Errorf("failed to convert list element type: %w", err)
	}
	
	return arrow.ListOf(elementType), nil
}

// convertMapType converts Iceberg map type to Arrow map type
func (sm *SchemaManager) convertMapType(mt *iceberg.MapType) (arrow.DataType, error) {
	keyType, err := sm.convertIcebergType(mt.KeyType)
	if err != nil {
		return nil, fmt.Errorf("failed to convert map key type: %w", err)
	}
	
	valueType, err := sm.convertIcebergType(mt.ValueType)
	if err != nil {
		return nil, fmt.Errorf("failed to convert map value type: %w", err)
	}
	
	return arrow.MapOf(keyType, valueType), nil
}

// convertStructType converts Iceberg struct type to Arrow struct type
func (sm *SchemaManager) convertStructType(st *iceberg.StructType) (arrow.DataType, error) {
	fields := make([]arrow.Field, 0, len(st.Fields()))
	
	for _, field := range st.Fields() {
		arrowField, err := sm.convertIcebergField(field)
		if err != nil {
			return nil, fmt.Errorf("failed to convert struct field %s: %w", field.Name, err)
		}
		fields = append(fields, arrowField)
	}
	
	return arrow.StructOf(fields...), nil
}

// ValidateData validates data against a given Arrow schema
func (sm *SchemaManager) ValidateData(data [][]interface{}, schema *arrow.Schema) error {
	if len(data) == 0 {
		return nil // Empty data is valid
	}

	if schema == nil {
		return fmt.Errorf("schema cannot be nil")
	}

	expectedColumns := len(schema.Fields())
	
	for rowIndex, row := range data {
		if len(row) != expectedColumns {
			return fmt.Errorf("row %d has %d columns, expected %d", rowIndex, len(row), expectedColumns)
		}
		
		if err := sm.validateRow(row, schema, rowIndex); err != nil {
			return fmt.Errorf("row %d validation failed: %w", rowIndex, err)
		}
	}
	
	return nil
}

// validateRow validates a single row against the schema
func (sm *SchemaManager) validateRow(row []interface{}, schema *arrow.Schema, rowIndex int) error {
	for colIndex, value := range row {
		field := schema.Field(colIndex)
		
		if err := sm.validateValue(value, field, rowIndex, colIndex); err != nil {
			return err
		}
	}
	
	return nil
}

// validateValue validates a single value against a field
func (sm *SchemaManager) validateValue(value interface{}, field arrow.Field, rowIndex, colIndex int) error {
	// Handle null values
	if value == nil {
		if !field.Nullable {
			return fmt.Errorf("field %s cannot be null (row %d, col %d)", field.Name, rowIndex, colIndex)
		}
		return nil
	}
	
	// Validate type compatibility
	if err := sm.validateType(value, field.Type, field.Name, rowIndex, colIndex); err != nil {
		return err
	}
	
	return nil
}

// validateType validates that a value is compatible with an Arrow type
func (sm *SchemaManager) validateType(value interface{}, arrowType arrow.DataType, fieldName string, rowIndex, colIndex int) error {
	switch arrowType.(type) {
	case *arrow.BooleanType:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("field %s expects boolean, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.Int32Type:
		if !sm.isInt32Compatible(value) {
			return fmt.Errorf("field %s expects int32, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.Int64Type:
		if !sm.isInt64Compatible(value) {
			return fmt.Errorf("field %s expects int64, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.Float32Type:
		if !sm.isFloat32Compatible(value) {
			return fmt.Errorf("field %s expects float32, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.Float64Type:
		if !sm.isFloat64Compatible(value) {
			return fmt.Errorf("field %s expects float64, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.StringType:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("field %s expects string, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.BinaryType:
		if _, ok := value.([]byte); !ok {
			return fmt.Errorf("field %s expects []byte, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.Date32Type:
		if !sm.isDateCompatible(value) {
			return fmt.Errorf("field %s expects date, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	case *arrow.TimestampType:
		if !sm.isTimestampCompatible(value) {
			return fmt.Errorf("field %s expects timestamp, got %T (row %d, col %d)", fieldName, value, rowIndex, colIndex)
		}
	default:
		// For complex types, we'll do basic validation
		return nil
	}
	
	return nil
}

// Type compatibility helpers
func (sm *SchemaManager) isInt32Compatible(value interface{}) bool {
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

func (sm *SchemaManager) isInt64Compatible(value interface{}) bool {
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

func (sm *SchemaManager) isFloat32Compatible(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func (sm *SchemaManager) isFloat64Compatible(value interface{}) bool {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	default:
		return false
	}
}

func (sm *SchemaManager) isDateCompatible(value interface{}) bool {
	switch value.(type) {
	case time.Time, string:
		return true
	default:
		return false
	}
}

func (sm *SchemaManager) isTimestampCompatible(value interface{}) bool {
	switch value.(type) {
	case time.Time, string:
		return true
	default:
		return false
	}
}
