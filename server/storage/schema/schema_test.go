package schema

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/iceberg-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewManager(t *testing.T) {
	config := DefaultParquetConfig()
	sm := NewManager(config)

	assert.NotNil(t, sm)
	assert.Equal(t, config, sm.config)
}

func TestConvertIcebergToArrowSchema_SimpleTypes(t *testing.T) {
	config := DefaultParquetConfig()
	sm := NewManager(config)

	// Create a simple Iceberg schema
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		iceberg.NestedField{ID: 3, Name: "active", Type: iceberg.PrimitiveTypes.Bool, Required: false},
		iceberg.NestedField{ID: 4, Name: "score", Type: iceberg.PrimitiveTypes.Float64, Required: false},
	)

	// Convert to Arrow schema
	arrowSchema, err := sm.ConvertIcebergToArrowSchema(schema)
	require.NoError(t, err)
	assert.NotNil(t, arrowSchema)

	// Verify fields
	assert.Equal(t, 4, len(arrowSchema.Fields()))

	// Check field types
	assert.Equal(t, "id", arrowSchema.Field(0).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int64, arrowSchema.Field(0).Type)
	assert.False(t, arrowSchema.Field(0).Nullable) // Required field

	assert.Equal(t, "name", arrowSchema.Field(1).Name)
	assert.Equal(t, arrow.BinaryTypes.String, arrowSchema.Field(1).Type)
	assert.True(t, arrowSchema.Field(1).Nullable) // Optional field

	assert.Equal(t, "active", arrowSchema.Field(2).Name)
	assert.Equal(t, arrow.FixedWidthTypes.Boolean, arrowSchema.Field(2).Type)
	assert.True(t, arrowSchema.Field(2).Nullable)

	assert.Equal(t, "score", arrowSchema.Field(3).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Float64, arrowSchema.Field(3).Type)
	assert.True(t, arrowSchema.Field(3).Nullable)
}

func TestConvertIcebergToArrowSchema_ComplexTypes(t *testing.T) {
	config := DefaultParquetConfig()
	sm := NewManager(config)

	// Create a complex Iceberg schema with nested types
	schema := iceberg.NewSchema(0,
		iceberg.NestedField{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		iceberg.NestedField{ID: 2, Name: "metadata", Type: &iceberg.StructType{
			FieldList: []iceberg.NestedField{
				{ID: 3, Name: "department", Type: iceberg.PrimitiveTypes.String, Required: false},
				{ID: 4, Name: "level", Type: iceberg.PrimitiveTypes.Int32, Required: false},
			},
		}, Required: false},
		iceberg.NestedField{ID: 5, Name: "tags", Type: &iceberg.ListType{
			ElementID:       6,
			Element:         iceberg.PrimitiveTypes.String,
			ElementRequired: false,
		}, Required: false},
	)

	// Convert to Arrow schema
	arrowSchema, err := sm.ConvertIcebergToArrowSchema(schema)
	require.NoError(t, err)
	assert.NotNil(t, arrowSchema)

	// Verify fields
	assert.Equal(t, 3, len(arrowSchema.Fields()))

	// Check basic field
	assert.Equal(t, "id", arrowSchema.Field(0).Name)
	assert.Equal(t, arrow.PrimitiveTypes.Int64, arrowSchema.Field(0).Type)

	// Check struct field
	assert.Equal(t, "metadata", arrowSchema.Field(1).Name)
	assert.IsType(t, &arrow.StructType{}, arrowSchema.Field(1).Type)

	// Check list field
	assert.Equal(t, "tags", arrowSchema.Field(2).Name)
	assert.IsType(t, &arrow.ListType{}, arrowSchema.Field(2).Type)
}

func TestValidateData_ValidData(t *testing.T) {
	config := DefaultParquetConfig()
	sm := NewManager(config)

	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	}, nil)

	// Valid data
	data := [][]interface{}{
		{int64(1), "Alice", true},
		{int64(2), "Bob", false},
		{int64(3), nil, true},
	}

	err := sm.ValidateData(data, schema)
	assert.NoError(t, err)
}

func TestValidateData_InvalidData(t *testing.T) {
	config := DefaultParquetConfig()
	sm := NewManager(config)

	// Create a simple schema
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	// Invalid data - wrong number of columns
	data := [][]interface{}{
		{int64(1), "Alice", "extra"},
		{int64(2), "Bob"},
	}

	err := sm.ValidateData(data, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "row has incorrect number of columns")

	// Invalid data - wrong type
	data = [][]interface{}{
		{int64(1), "Alice"},
		{"invalid", "Bob"}, // id should be int64
	}

	err = sm.ValidateData(data, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "expects int64")

	// Invalid data - null in required field
	data = [][]interface{}{
		{int64(1), "Alice"},
		{int64(2), nil}, // name is required
	}

	err = sm.ValidateData(data, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be null")
}

func TestValidateData_EmptyData(t *testing.T) {
	config := DefaultParquetConfig()
	sm := NewManager(config)

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Empty data should be valid
	data := [][]interface{}{}
	err := sm.ValidateData(data, schema)
	assert.NoError(t, err)

	// Nil data should be valid
	err = sm.ValidateData(nil, schema)
	assert.NoError(t, err)
}

func TestValidateData_NilSchema(t *testing.T) {
	config := DefaultParquetConfig()
	sm := NewManager(config)

	data := [][]interface{}{
		{int64(1), "Alice"},
	}

	err := sm.ValidateData(data, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "schema cannot be nil")
}
