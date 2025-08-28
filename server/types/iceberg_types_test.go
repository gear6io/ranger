package types

import (
	"testing"
)

func TestPrimitiveType(t *testing.T) {
	tests := []struct {
		name        string
		typeName    string
		precision   int
		scale       int
		expectValid bool
		expectStr   string
	}{
		{"valid string", IcebergString, 0, 0, true, "string"},
		{"valid int32", IcebergInt32, 0, 0, true, "int32"},
		{"valid decimal", IcebergDecimal, 10, 2, true, "decimal(10,2)"},
		{"invalid decimal precision", IcebergDecimal, 0, 2, false, ""},
		{"invalid decimal scale", IcebergDecimal, 10, -1, false, ""},
		{"invalid decimal scale > precision", IcebergDecimal, 5, 10, false, ""},
		{"invalid type name", "invalid", 0, 0, false, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pt := &PrimitiveType{
				TypeName:  tt.typeName,
				Precision: tt.precision,
				Scale:     tt.scale,
			}

			err := pt.Validate()
			if tt.expectValid && err != nil {
				t.Errorf("expected valid type, got error: %v", err)
			}
			if !tt.expectValid && err == nil {
				t.Errorf("expected invalid type, got no error")
			}

			if tt.expectValid {
				if pt.String() != tt.expectStr {
					t.Errorf("expected string %s, got %s", tt.expectStr, pt.String())
				}
				if pt.IsComplex() {
					t.Errorf("primitive type should not be complex")
				}
				if len(pt.GetNestedTypes()) != 0 {
					t.Errorf("primitive type should have no nested types")
				}
			}
		})
	}
}

func TestListType(t *testing.T) {
	tests := []struct {
		name        string
		elementType IcebergType
		expectValid bool
		expectStr   string
	}{
		{
			"valid list of strings",
			&PrimitiveType{TypeName: IcebergString},
			true,
			"list<string>",
		},
		{
			"valid nested list",
			&ListType{ElementType: &PrimitiveType{TypeName: IcebergInt32}},
			true,
			"list<list<int32>>",
		},
		{
			"nil element type",
			nil,
			false,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lt := &ListType{ElementType: tt.elementType}

			err := lt.Validate()
			if tt.expectValid && err != nil {
				t.Errorf("expected valid type, got error: %v", err)
			}
			if !tt.expectValid && err == nil {
				t.Errorf("expected invalid type, got no error")
			}

			if tt.expectValid {
				if lt.String() != tt.expectStr {
					t.Errorf("expected string %s, got %s", tt.expectStr, lt.String())
				}
				if !lt.IsComplex() {
					t.Errorf("list type should be complex")
				}
				if len(lt.GetNestedTypes()) != 1 {
					t.Errorf("list type should have exactly 1 nested type")
				}
			}
		})
	}
}

func TestMapType(t *testing.T) {
	tests := []struct {
		name        string
		keyType     IcebergType
		valueType   IcebergType
		expectValid bool
		expectStr   string
	}{
		{
			"valid map",
			&PrimitiveType{TypeName: IcebergString},
			&PrimitiveType{TypeName: IcebergInt32},
			true,
			"map<string,int32>",
		},
		{
			"nil key type",
			nil,
			&PrimitiveType{TypeName: IcebergInt32},
			false,
			"",
		},
		{
			"nil value type",
			&PrimitiveType{TypeName: IcebergString},
			nil,
			false,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mt := &MapType{KeyType: tt.keyType, ValueType: tt.valueType}

			err := mt.Validate()
			if tt.expectValid && err != nil {
				t.Errorf("expected valid type, got error: %v", err)
			}
			if !tt.expectValid && err == nil {
				t.Errorf("expected invalid type, got no error")
			}

			if tt.expectValid {
				if mt.String() != tt.expectStr {
					t.Errorf("expected string %s, got %s", tt.expectStr, mt.String())
				}
				if !mt.IsComplex() {
					t.Errorf("map type should be complex")
				}
				if len(mt.GetNestedTypes()) != 2 {
					t.Errorf("map type should have exactly 2 nested types")
				}
			}
		})
	}
}

func TestStructType(t *testing.T) {
	tests := []struct {
		name        string
		fields      []StructField
		expectValid bool
		expectStr   string
	}{
		{
			"valid struct",
			[]StructField{
				{Name: "name", Type: &PrimitiveType{TypeName: IcebergString}},
				{Name: "age", Type: &PrimitiveType{TypeName: IcebergInt32}},
			},
			true,
			"struct<name:string,age:int32>",
		},
		{
			"empty struct",
			[]StructField{},
			false,
			"",
		},
		{
			"duplicate field names",
			[]StructField{
				{Name: "name", Type: &PrimitiveType{TypeName: IcebergString}},
				{Name: "name", Type: &PrimitiveType{TypeName: IcebergInt32}},
			},
			false,
			"",
		},
		{
			"empty field name",
			[]StructField{
				{Name: "", Type: &PrimitiveType{TypeName: IcebergString}},
			},
			false,
			"",
		},
		{
			"nil field type",
			[]StructField{
				{Name: "field", Type: nil},
			},
			false,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &StructType{Fields: tt.fields}

			err := st.Validate()
			if tt.expectValid && err != nil {
				t.Errorf("expected valid type, got error: %v", err)
			}
			if !tt.expectValid && err == nil {
				t.Errorf("expected invalid type, got no error")
			}

			if tt.expectValid {
				if st.String() != tt.expectStr {
					t.Errorf("expected string %s, got %s", tt.expectStr, st.String())
				}
				if !st.IsComplex() {
					t.Errorf("struct type should be complex")
				}
				if len(st.GetNestedTypes()) != len(tt.fields) {
					t.Errorf("struct type should have %d nested types, got %d", len(tt.fields), len(st.GetNestedTypes()))
				}
			}
		})
	}
}

func TestIcebergTypeValidator(t *testing.T) {
	validator := NewIcebergTypeValidator()

	tests := []struct {
		name        string
		typeStr     string
		expectValid bool
	}{
		// Primitive types
		{"valid string", "string", true},
		{"valid int32", "int32", true},
		{"valid boolean", "boolean", true},
		{"invalid primitive", "varchar", false},

		// Decimal types
		{"valid decimal", "decimal(10,2)", true},
		{"invalid decimal format", "decimal(10)", false},
		{"invalid decimal precision", "decimal(0,2)", false},

		// List types
		{"valid list", "list<string>", true},
		{"valid nested list", "list<list<int32>>", true},
		{"invalid list format", "list<>", false},
		{"invalid list element", "list<varchar>", false},

		// Map types
		{"valid map", "map<string,int32>", true},
		{"valid nested map", "map<string,list<int32>>", true},
		{"invalid map format", "map<string>", false},
		{"invalid map key", "map<varchar,int32>", false},

		// Struct types
		{"valid struct", "struct<name:string,age:int32>", true},
		{"valid nested struct", "struct<person:struct<name:string,age:int32>>", true},
		{"invalid struct format", "struct<name>", false},
		{"invalid struct field type", "struct<name:varchar>", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isValid := validator.IsValidType(tt.typeStr)
			if isValid != tt.expectValid {
				t.Errorf("expected IsValidType(%s) = %v, got %v", tt.typeStr, tt.expectValid, isValid)
			}

			if tt.expectValid {
				// Test parsing
				parsedType, err := validator.ParseType(tt.typeStr)
				if err != nil {
					t.Errorf("expected successful parsing, got error: %v", err)
				}
				if parsedType == nil {
					t.Errorf("expected non-nil parsed type")
				}

				// Test validation
				err = validator.ValidateComplexType(tt.typeStr)
				if err != nil {
					t.Errorf("expected successful validation, got error: %v", err)
				}
			}
		})
	}
}

func TestTypeConverter(t *testing.T) {
	converter := NewTypeConverter()

	tests := []struct {
		name         string
		icebergType  IcebergType
		expectString string
		expectError  bool
	}{
		{
			"primitive type",
			&PrimitiveType{TypeName: IcebergString},
			"string",
			false,
		},
		{
			"decimal type",
			&PrimitiveType{TypeName: IcebergDecimal, Precision: 10, Scale: 2},
			"decimal(10,2)",
			false,
		},
		{
			"list type",
			&ListType{ElementType: &PrimitiveType{TypeName: IcebergString}},
			"list<string>",
			false,
		},
		{
			"nil type",
			nil,
			"",
			true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registryFormat, err := converter.ConvertToRegistryFormat(tt.icebergType)

			if tt.expectError && err == nil {
				t.Errorf("expected error, got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("expected no error, got: %v", err)
			}

			if !tt.expectError {
				if registryFormat != tt.expectString {
					t.Errorf("expected registry format %s, got %s", tt.expectString, registryFormat)
				}

				// Test round-trip conversion
				backToIceberg, err := converter.ConvertFromRegistryFormat(registryFormat)
				if err != nil {
					t.Errorf("failed round-trip conversion: %v", err)
				}
				if backToIceberg.String() != tt.expectString {
					t.Errorf("round-trip failed: expected %s, got %s", tt.expectString, backToIceberg.String())
				}
			}
		})
	}
}

func TestUtilityFunctions(t *testing.T) {
	t.Run("GetAllSupportedTypes", func(t *testing.T) {
		types := GetAllSupportedTypes()
		if len(types) == 0 {
			t.Errorf("expected non-empty list of supported types")
		}

		// Check that it includes both primitive and complex examples
		hasString := false
		hasDecimal := false
		hasList := false
		for _, typeStr := range types {
			if typeStr == "string" {
				hasString = true
			}
			if typeStr == "decimal(10,2)" {
				hasDecimal = true
			}
			if typeStr == "list<string>" {
				hasList = true
			}
		}

		if !hasString {
			t.Errorf("expected 'string' in supported types")
		}
		if !hasDecimal {
			t.Errorf("expected 'decimal(10,2)' in supported types")
		}
		if !hasList {
			t.Errorf("expected 'list<string>' in supported types")
		}
	})

	t.Run("ValidateTypeString", func(t *testing.T) {
		err := ValidateTypeString("string")
		if err != nil {
			t.Errorf("expected valid type string, got error: %v", err)
		}

		err = ValidateTypeString("invalid_type")
		if err == nil {
			t.Errorf("expected error for invalid type string")
		}
	})

	t.Run("ParseAndValidateType", func(t *testing.T) {
		parsedType, err := ParseAndValidateType("list<string>")
		if err != nil {
			t.Errorf("expected successful parsing, got error: %v", err)
		}
		if parsedType == nil {
			t.Errorf("expected non-nil parsed type")
		}
		if !parsedType.IsComplex() {
			t.Errorf("expected complex type")
		}
	})

	t.Run("GetTypeComplexity", func(t *testing.T) {
		primitiveType := &PrimitiveType{TypeName: IcebergString}
		if GetTypeComplexity(primitiveType) != 0 {
			t.Errorf("expected complexity 0 for primitive type")
		}

		listType := &ListType{ElementType: primitiveType}
		if GetTypeComplexity(listType) != 1 {
			t.Errorf("expected complexity 1 for list type")
		}

		nestedListType := &ListType{ElementType: listType}
		if GetTypeComplexity(nestedListType) != 2 {
			t.Errorf("expected complexity 2 for nested list type")
		}
	})

	t.Run("IsCompatibleType", func(t *testing.T) {
		stringType1 := &PrimitiveType{TypeName: IcebergString}
		stringType2 := &PrimitiveType{TypeName: IcebergString}
		intType := &PrimitiveType{TypeName: IcebergInt32}
		floatType := &PrimitiveType{TypeName: IcebergFloat32}

		if !IsCompatibleType(stringType1, stringType2) {
			t.Errorf("expected string types to be compatible")
		}

		if !IsCompatibleType(intType, floatType) {
			t.Errorf("expected numeric types to be compatible")
		}

		if IsCompatibleType(stringType1, intType) {
			t.Errorf("expected string and int types to be incompatible")
		}

		if IsCompatibleType(nil, stringType1) {
			t.Errorf("expected nil type to be incompatible")
		}
	})
}
