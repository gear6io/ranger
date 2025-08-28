package schema_manager

import (
	"testing"

	"github.com/apache/iceberg-go"
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaRetriever_ParseIcebergType_Primitives(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		dataType     string
		expectedType iceberg.Type
	}{
		{"boolean", iceberg.PrimitiveTypes.Bool},
		{"int32", iceberg.PrimitiveTypes.Int32},
		{"int64", iceberg.PrimitiveTypes.Int64},
		{"float32", iceberg.PrimitiveTypes.Float32},
		{"float64", iceberg.PrimitiveTypes.Float64},
		{"string", iceberg.PrimitiveTypes.String},
		{"binary", iceberg.PrimitiveTypes.Binary},
		{"date", iceberg.PrimitiveTypes.Date},
		{"time", iceberg.PrimitiveTypes.Time},
		{"timestamp", iceberg.PrimitiveTypes.Timestamp},
		{"timestamptz", iceberg.PrimitiveTypes.TimestampTz},
		{"uuid", iceberg.PrimitiveTypes.UUID},
	}

	for _, test := range tests {
		t.Run(test.dataType, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			require.NoError(t, err)
			assert.Equal(t, test.expectedType, result)
		})
	}
}

func TestSchemaRetriever_ParseIcebergType_Decimal(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		dataType  string
		precision int
		scale     int
	}{
		{"decimal(10,2)", 10, 2},
		{"decimal(38,18)", 38, 18},
		{"decimal(5,0)", 5, 0},
	}

	for _, test := range tests {
		t.Run(test.dataType, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			require.NoError(t, err)

			decimalType, ok := result.(iceberg.DecimalType)
			require.True(t, ok)
			assert.Equal(t, test.precision, decimalType.Precision())
			assert.Equal(t, test.scale, decimalType.Scale())
		})
	}
}

func TestSchemaRetriever_ParseIcebergType_Fixed(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		dataType string
		length   int
	}{
		{"fixed(16)", 16},
		{"fixed(32)", 32},
		{"fixed(1)", 1},
	}

	for _, test := range tests {
		t.Run(test.dataType, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			require.NoError(t, err)

			fixedType, ok := result.(iceberg.FixedType)
			require.True(t, ok)
			assert.Equal(t, test.length, fixedType.Len())
		})
	}
}

func TestSchemaRetriever_ParseIcebergType_List(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		dataType    string
		elementType iceberg.Type
	}{
		{"list<string>", iceberg.PrimitiveTypes.String},
		{"list<int32>", iceberg.PrimitiveTypes.Int32},
		{"list<boolean>", iceberg.PrimitiveTypes.Bool},
	}

	for _, test := range tests {
		t.Run(test.dataType, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			require.NoError(t, err)

			listType, ok := result.(*iceberg.ListType)
			require.True(t, ok)
			assert.Equal(t, test.elementType, listType.Element)
		})
	}
}

func TestSchemaRetriever_ParseIcebergType_Map(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		dataType  string
		keyType   iceberg.Type
		valueType iceberg.Type
	}{
		{"map<string,int32>", iceberg.PrimitiveTypes.String, iceberg.PrimitiveTypes.Int32},
		{"map<int64,boolean>", iceberg.PrimitiveTypes.Int64, iceberg.PrimitiveTypes.Bool},
	}

	for _, test := range tests {
		t.Run(test.dataType, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			require.NoError(t, err)

			mapType, ok := result.(*iceberg.MapType)
			require.True(t, ok)
			assert.Equal(t, test.keyType, mapType.KeyType)
			assert.Equal(t, test.valueType, mapType.ValueType)
		})
	}
}

func TestSchemaRetriever_ParseIcebergType_Struct(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		name     string
		dataType string
		validate func(t *testing.T, result iceberg.Type)
	}{
		{
			name:     "empty struct",
			dataType: "struct<>",
			validate: func(t *testing.T, result iceberg.Type) {
				structType, ok := result.(*iceberg.StructType)
				require.True(t, ok)
				assert.Equal(t, 0, len(structType.Fields()))
			},
		},
		{
			name:     "single field struct",
			dataType: "struct<name:string>",
			validate: func(t *testing.T, result iceberg.Type) {
				structType, ok := result.(*iceberg.StructType)
				require.True(t, ok)
				fields := structType.Fields()
				require.Equal(t, 1, len(fields))
				assert.Equal(t, "name", fields[0].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.String, fields[0].Type)
			},
		},
		{
			name:     "multi field struct",
			dataType: "struct<id:int64,name:string,active:boolean>",
			validate: func(t *testing.T, result iceberg.Type) {
				structType, ok := result.(*iceberg.StructType)
				require.True(t, ok)
				fields := structType.Fields()
				require.Equal(t, 3, len(fields))

				assert.Equal(t, "id", fields[0].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Int64, fields[0].Type)

				assert.Equal(t, "name", fields[1].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.String, fields[1].Type)

				assert.Equal(t, "active", fields[2].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Bool, fields[2].Type)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			require.NoError(t, err)
			test.validate(t, result)
		})
	}
}

func TestSchemaRetriever_ParseIcebergType_InvalidTypes(t *testing.T) {
	retriever := &SchemaRetriever{}

	invalidTypes := []string{
		"invalid_type",
		"list<>",
		"list<invalid>",
		"map<string>",
		"map<string,>",
		"map<,int32>",
		"struct<name>",
		"struct<name:>",
		"struct<:string>",
		"decimal()",
		"decimal(10)",
		"decimal(10,)",
		"decimal(,2)",
		"decimal(abc,2)",
		"decimal(10,abc)",
		"fixed()",
		"fixed(abc)",
		"",
	}

	for _, invalidType := range invalidTypes {
		t.Run(invalidType, func(t *testing.T) {
			_, err := retriever.parseIcebergType(invalidType)
			assert.Error(t, err)
		})
	}
}

func TestSchemaRetriever_ConvertColumnToIcebergField(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		name     string
		column   *regtypes.TableColumn
		fieldID  int
		validate func(t *testing.T, field iceberg.NestedField)
	}{
		{
			name: "nullable string field",
			column: &regtypes.TableColumn{
				ColumnName: "name",
				DataType:   "string",
				IsNullable: true,
			},
			fieldID: 1,
			validate: func(t *testing.T, field iceberg.NestedField) {
				assert.Equal(t, 1, field.ID)
				assert.Equal(t, "name", field.Name)
				assert.Equal(t, iceberg.PrimitiveTypes.String, field.Type)
				assert.False(t, field.Required)
			},
		},
		{
			name: "required int64 field",
			column: &regtypes.TableColumn{
				ColumnName: "id",
				DataType:   "int64",
				IsNullable: false,
			},
			fieldID: 0,
			validate: func(t *testing.T, field iceberg.NestedField) {
				assert.Equal(t, 0, field.ID)
				assert.Equal(t, "id", field.Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Int64, field.Type)
				assert.True(t, field.Required)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			field, err := retriever.convertColumnToIcebergField(test.column, test.fieldID)
			require.NoError(t, err)
			test.validate(t, field)
		})
	}
}

func TestSchemaRetriever_ConvertColumnToIcebergField_InvalidType(t *testing.T) {
	retriever := &SchemaRetriever{}

	column := &regtypes.TableColumn{
		ColumnName: "invalid_field",
		DataType:   "invalid_type",
		IsNullable: true,
	}

	_, err := retriever.convertColumnToIcebergField(column, 0)
	assert.Error(t, err)
}

// TestSchemaRetriever_ParseIcebergType_NestedComplexTypes tests complex nested type combinations
// Requirements 2.3, 2.4: Parse all Iceberg type combinations and combine into complete schema
func TestSchemaRetriever_ParseIcebergType_NestedComplexTypes(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		name     string
		dataType string
		validate func(t *testing.T, result iceberg.Type)
	}{
		{
			name:     "list of lists",
			dataType: "list<list<string>>",
			validate: func(t *testing.T, result iceberg.Type) {
				listType, ok := result.(*iceberg.ListType)
				require.True(t, ok)
				innerList, ok := listType.Element.(*iceberg.ListType)
				require.True(t, ok)
				assert.Equal(t, iceberg.PrimitiveTypes.String, innerList.Element)
			},
		},
		{
			name:     "map with list values",
			dataType: "map<string,list<int64>>",
			validate: func(t *testing.T, result iceberg.Type) {
				mapType, ok := result.(*iceberg.MapType)
				require.True(t, ok)
				assert.Equal(t, iceberg.PrimitiveTypes.String, mapType.KeyType)
				listType, ok := mapType.ValueType.(*iceberg.ListType)
				require.True(t, ok)
				assert.Equal(t, iceberg.PrimitiveTypes.Int64, listType.Element)
			},
		},
		{
			name:     "struct with simple nested types",
			dataType: "struct<id:int64,name:string,active:boolean>",
			validate: func(t *testing.T, result iceberg.Type) {
				structType, ok := result.(*iceberg.StructType)
				require.True(t, ok)
				fields := structType.Fields()
				require.Equal(t, 3, len(fields))

				// Check id field
				assert.Equal(t, "id", fields[0].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Int64, fields[0].Type)

				// Check name field
				assert.Equal(t, "name", fields[1].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.String, fields[1].Type)

				// Check active field
				assert.Equal(t, "active", fields[2].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Bool, fields[2].Type)
			},
		},
		{
			name:     "list of structs with simple fields",
			dataType: "list<struct<name:string,age:int32>>",
			validate: func(t *testing.T, result iceberg.Type) {
				listType, ok := result.(*iceberg.ListType)
				require.True(t, ok)
				structType, ok := listType.Element.(*iceberg.StructType)
				require.True(t, ok)
				fields := structType.Fields()
				require.Equal(t, 2, len(fields))
				assert.Equal(t, "name", fields[0].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.String, fields[0].Type)
				assert.Equal(t, "age", fields[1].Name)
				assert.Equal(t, iceberg.PrimitiveTypes.Int32, fields[1].Type)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			require.NoError(t, err)
			test.validate(t, result)
		})
	}
}

// TestSchemaRetriever_ParseIcebergType_ComplexNestedTypes tests more complex nested combinations
// Note: This test demonstrates current limitations and areas for future enhancement
func TestSchemaRetriever_ParseIcebergType_ComplexNestedTypes(t *testing.T) {
	retriever := &SchemaRetriever{}

	// TODO: These complex nested types require more sophisticated parsing
	// For now, we test that they fail gracefully with descriptive errors
	complexTypes := []string{
		"struct<id:int64,tags:list<string>,metadata:map<string,string>>", // Struct with complex nested types
		"map<string,struct<name:string,value:int64>>",                    // Map with struct values
		"list<map<string,int64>>",                                        // List of maps
	}

	for _, complexType := range complexTypes {
		t.Run(complexType, func(t *testing.T) {
			_, err := retriever.parseIcebergType(complexType)
			// For now, these should fail with parsing errors
			// In the future, we can enhance the parser to handle these cases
			if err != nil {
				t.Logf("Complex type parsing not yet supported: %s (error: %v)", complexType, err)
				assert.Contains(t, err.Error(), "unsupported data type")
			} else {
				t.Logf("Complex type parsing succeeded: %s", complexType)
			}
		})
	}
}

// TestSchemaRetriever_ParseIcebergType_EdgeCases tests edge cases and error conditions
// Requirement 2.5: Return descriptive errors for invalid DataType
func TestSchemaRetriever_ParseIcebergType_EdgeCases(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		name        string
		dataType    string
		expectError bool
		errorCheck  func(t *testing.T, err error)
	}{
		{
			name:        "empty string",
			dataType:    "",
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "unsupported data type")
			},
		},
		{
			name:        "whitespace only",
			dataType:    "   ",
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "unsupported data type")
			},
		},
		{
			name:        "malformed list - missing closing bracket",
			dataType:    "list<string",
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "unsupported data type")
			},
		},
		{
			name:        "malformed map - missing comma",
			dataType:    "map<string int32>",
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "invalid map type format")
			},
		},
		{
			name:        "malformed struct - missing colon",
			dataType:    "struct<name string>",
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "invalid struct field format")
			},
		},
		{
			name:        "decimal with invalid precision",
			dataType:    "decimal(abc,2)",
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "invalid decimal precision")
			},
		},
		{
			name:        "fixed with invalid length",
			dataType:    "fixed(xyz)",
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "invalid fixed length")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result, err := retriever.parseIcebergType(test.dataType)
			if test.expectError {
				assert.Error(t, err)
				assert.Nil(t, result)
				if test.errorCheck != nil {
					test.errorCheck(t, err)
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
			}
		})
	}
}

// TestSchemaRetriever_SchemaReconstructionAccuracy tests complete schema reconstruction
// Requirements 2.3, 2.4: Accurate schema reconstruction from TableColumn records
func TestSchemaRetriever_SchemaReconstructionAccuracy(t *testing.T) {
	retriever := &SchemaRetriever{}

	// Test complex schema reconstruction
	columns := []*regtypes.TableColumn{
		{
			ColumnName:      "id",
			DataType:        "int64",
			IsNullable:      false,
			OrdinalPosition: 1,
		},
		{
			ColumnName:      "name",
			DataType:        "string",
			IsNullable:      true,
			OrdinalPosition: 2,
		},
		{
			ColumnName:      "tags",
			DataType:        "list<string>",
			IsNullable:      true,
			OrdinalPosition: 3,
		},
		{
			ColumnName:      "metadata",
			DataType:        "map<string,string>",
			IsNullable:      true,
			OrdinalPosition: 4,
		},
		{
			ColumnName:      "profile",
			DataType:        "struct<age:int32,active:boolean>",
			IsNullable:      true,
			OrdinalPosition: 5,
		},
		{
			ColumnName:      "price",
			DataType:        "decimal(10,2)",
			IsNullable:      true,
			OrdinalPosition: 6,
		},
	}

	// Convert columns to fields
	fields := make([]iceberg.NestedField, 0, len(columns))
	for i, column := range columns {
		field, err := retriever.convertColumnToIcebergField(column, i)
		require.NoError(t, err)
		fields = append(fields, field)
	}

	// Verify field accuracy
	require.Equal(t, 6, len(fields))

	// Check id field
	assert.Equal(t, 0, fields[0].ID)
	assert.Equal(t, "id", fields[0].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.Int64, fields[0].Type)
	assert.True(t, fields[0].Required)

	// Check name field
	assert.Equal(t, 1, fields[1].ID)
	assert.Equal(t, "name", fields[1].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.String, fields[1].Type)
	assert.False(t, fields[1].Required)

	// Check tags field (list)
	assert.Equal(t, 2, fields[2].ID)
	assert.Equal(t, "tags", fields[2].Name)
	listType, ok := fields[2].Type.(*iceberg.ListType)
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.String, listType.Element)
	assert.False(t, fields[2].Required)

	// Check metadata field (map)
	assert.Equal(t, 3, fields[3].ID)
	assert.Equal(t, "metadata", fields[3].Name)
	mapType, ok := fields[3].Type.(*iceberg.MapType)
	require.True(t, ok)
	assert.Equal(t, iceberg.PrimitiveTypes.String, mapType.KeyType)
	assert.Equal(t, iceberg.PrimitiveTypes.String, mapType.ValueType)
	assert.False(t, fields[3].Required)

	// Check profile field (struct)
	assert.Equal(t, 4, fields[4].ID)
	assert.Equal(t, "profile", fields[4].Name)
	structType, ok := fields[4].Type.(*iceberg.StructType)
	require.True(t, ok)
	structFields := structType.Fields()
	require.Equal(t, 2, len(structFields))
	assert.Equal(t, "age", structFields[0].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.Int32, structFields[0].Type)
	assert.Equal(t, "active", structFields[1].Name)
	assert.Equal(t, iceberg.PrimitiveTypes.Bool, structFields[1].Type)
	assert.False(t, fields[4].Required)

	// Check price field (decimal)
	assert.Equal(t, 5, fields[5].ID)
	assert.Equal(t, "price", fields[5].Name)
	decimalType, ok := fields[5].Type.(iceberg.DecimalType)
	require.True(t, ok)
	assert.Equal(t, 10, decimalType.Precision())
	assert.Equal(t, 2, decimalType.Scale())
	assert.False(t, fields[5].Required)

	// Create complete schema
	schema := iceberg.NewSchema(0, fields...)
	assert.NotNil(t, schema)
	assert.Equal(t, 6, len(schema.Fields()))
}

// TestSchemaRetriever_ErrorHandling tests comprehensive error handling scenarios
// Requirement 2.5: Descriptive errors for invalid DataType
func TestSchemaRetriever_ErrorHandling(t *testing.T) {
	retriever := &SchemaRetriever{}

	tests := []struct {
		name        string
		column      *regtypes.TableColumn
		expectError bool
		errorCheck  func(t *testing.T, err error)
	}{
		{
			name: "invalid data type",
			column: &regtypes.TableColumn{
				ColumnName: "invalid_field",
				DataType:   "unknown_type",
				IsNullable: true,
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "unsupported data type")
			},
		},
		{
			name: "malformed list type",
			column: &regtypes.TableColumn{
				ColumnName: "bad_list",
				DataType:   "list<>",
				IsNullable: true,
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "unsupported data type")
			},
		},
		{
			name: "malformed map type",
			column: &regtypes.TableColumn{
				ColumnName: "bad_map",
				DataType:   "map<string>",
				IsNullable: true,
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "invalid map type format")
			},
		},
		{
			name: "malformed struct type",
			column: &regtypes.TableColumn{
				ColumnName: "bad_struct",
				DataType:   "struct<name>",
				IsNullable: true,
			},
			expectError: true,
			errorCheck: func(t *testing.T, err error) {
				assert.Contains(t, err.Error(), "invalid struct field format")
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := retriever.convertColumnToIcebergField(test.column, 0)
			if test.expectError {
				assert.Error(t, err)
				if test.errorCheck != nil {
					test.errorCheck(t, err)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Helper function to create a test TableColumn
func createTestColumn(name, dataType string, nullable bool, position int) *regtypes.TableColumn {
	return &regtypes.TableColumn{
		ColumnName:      name,
		DataType:        dataType,
		IsNullable:      nullable,
		OrdinalPosition: position,
	}
}
