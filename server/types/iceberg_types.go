package types

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/gear6io/ranger/pkg/errors"
)

// Iceberg primitive types - comprehensive set covering all Iceberg specifications
const (
	IcebergBoolean     = "boolean"
	IcebergInt32       = "int32"
	IcebergInt64       = "int64"
	IcebergFloat32     = "float32"
	IcebergFloat64     = "float64"
	IcebergDecimal     = "decimal"
	IcebergString      = "string"
	IcebergBinary      = "binary"
	IcebergDate        = "date"
	IcebergTime        = "time"
	IcebergTimestamp   = "timestamp"
	IcebergTimestampTz = "timestamptz"
	IcebergUUID        = "uuid"
	// Complex types
	IcebergList   = "list"
	IcebergMap    = "map"
	IcebergStruct = "struct"
)

// IcebergType represents a parsed Iceberg type with validation capabilities
type IcebergType interface {
	String() string
	IsComplex() bool
	GetNestedTypes() []IcebergType
	Validate() error
}

// PrimitiveType represents a simple Iceberg type
type PrimitiveType struct {
	TypeName  string
	Precision int // For decimal types
	Scale     int // For decimal types
	Length    int // For fixed-length types
}

func (pt *PrimitiveType) String() string {
	switch pt.TypeName {
	case IcebergDecimal:
		if pt.Precision > 0 && pt.Scale >= 0 {
			return fmt.Sprintf("decimal(%d,%d)", pt.Precision, pt.Scale)
		}
		return pt.TypeName
	default:
		return pt.TypeName
	}
}

func (pt *PrimitiveType) IsComplex() bool {
	return false
}

func (pt *PrimitiveType) GetNestedTypes() []IcebergType {
	return nil
}

func (pt *PrimitiveType) Validate() error {
	if !isValidPrimitiveType(pt.TypeName) {
		return errors.New(ErrInvalidPrimitiveType, "invalid primitive type", nil).
			AddContext("type_name", pt.TypeName)
	}

	if pt.TypeName == IcebergDecimal {
		if pt.Precision <= 0 {
			return errors.New(ErrInvalidDecimalPrecision, "decimal precision must be positive", nil).
				AddContext("precision", pt.Precision)
		}
		if pt.Scale < 0 {
			return errors.New(ErrInvalidDecimalScale, "decimal scale must be non-negative", nil).
				AddContext("scale", pt.Scale)
		}
		if pt.Scale > pt.Precision {
			return errors.New(ErrDecimalScaleExceedsPrecision, "decimal scale cannot exceed precision", nil).
				AddContext("scale", pt.Scale).
				AddContext("precision", pt.Precision)
		}
	}

	return nil
}

// ListType represents an Iceberg list type
type ListType struct {
	ElementType IcebergType
}

func (lt *ListType) String() string {
	return fmt.Sprintf("list<%s>", lt.ElementType.String())
}

func (lt *ListType) IsComplex() bool {
	return true
}

func (lt *ListType) GetNestedTypes() []IcebergType {
	return []IcebergType{lt.ElementType}
}

func (lt *ListType) Validate() error {
	if lt.ElementType == nil {
		return errors.New(ErrInvalidListElement, "list element type cannot be nil", nil)
	}
	return lt.ElementType.Validate()
}

// MapType represents an Iceberg map type
type MapType struct {
	KeyType   IcebergType
	ValueType IcebergType
}

func (mt *MapType) String() string {
	return fmt.Sprintf("map<%s,%s>", mt.KeyType.String(), mt.ValueType.String())
}

func (mt *MapType) IsComplex() bool {
	return true
}

func (mt *MapType) GetNestedTypes() []IcebergType {
	return []IcebergType{mt.KeyType, mt.ValueType}
}

func (mt *MapType) Validate() error {
	if mt.KeyType == nil {
		return errors.New(ErrInvalidMapKeyType, "map key type cannot be nil", nil)
	}
	if mt.ValueType == nil {
		return errors.New(ErrInvalidMapValueType, "map value type cannot be nil", nil)
	}

	if err := mt.KeyType.Validate(); err != nil {
		return errors.New(ErrInvalidMapKeyType, "invalid map key type", err)
	}

	return mt.ValueType.Validate()
}

// StructField represents a field in a struct type
type StructField struct {
	Name string
	Type IcebergType
}

// StructType represents an Iceberg struct type
type StructType struct {
	Fields []StructField
}

func (st *StructType) String() string {
	var fieldStrs []string
	for _, field := range st.Fields {
		fieldStrs = append(fieldStrs, fmt.Sprintf("%s:%s", field.Name, field.Type.String()))
	}
	return fmt.Sprintf("struct<%s>", strings.Join(fieldStrs, ","))
}

func (st *StructType) IsComplex() bool {
	return true
}

func (st *StructType) GetNestedTypes() []IcebergType {
	var types []IcebergType
	for _, field := range st.Fields {
		types = append(types, field.Type)
	}
	return types
}

func (st *StructType) Validate() error {
	if len(st.Fields) == 0 {
		return errors.New(ErrInvalidStructField, "struct must have at least one field", nil)
	}

	fieldNames := make(map[string]bool)
	for i, field := range st.Fields {
		if field.Name == "" {
			return errors.New(ErrInvalidStructField, "struct field name cannot be empty", nil).
				AddContext("field_index", i)
		}

		if fieldNames[field.Name] {
			return errors.New(ErrInvalidStructField, "duplicate struct field name", nil).
				AddContext("field_name", field.Name)
		}
		fieldNames[field.Name] = true

		if field.Type == nil {
			return errors.New(ErrInvalidStructField, "struct field type cannot be nil", nil).
				AddContext("field_name", field.Name)
		}

		if err := field.Type.Validate(); err != nil {
			return errors.New(ErrInvalidStructField, "struct field validation failed", err).
				AddContext("field_name", field.Name)
		}
	}

	return nil
}

// IcebergTypeValidator provides validation and parsing capabilities for Iceberg types
type IcebergTypeValidator interface {
	IsValidType(typeStr string) bool
	ParseType(typeStr string) (IcebergType, error)
	ValidateComplexType(typeStr string) error
	GetSupportedTypes() []string
}

// DefaultIcebergTypeValidator implements IcebergTypeValidator
type DefaultIcebergTypeValidator struct {
	primitiveTypes map[string]bool
}

// NewIcebergTypeValidator creates a new type validator instance
func NewIcebergTypeValidator() IcebergTypeValidator {
	primitiveTypes := map[string]bool{
		IcebergBoolean:     true,
		IcebergInt32:       true,
		IcebergInt64:       true,
		IcebergFloat32:     true,
		IcebergFloat64:     true,
		IcebergDecimal:     true,
		IcebergString:      true,
		IcebergBinary:      true,
		IcebergDate:        true,
		IcebergTime:        true,
		IcebergTimestamp:   true,
		IcebergTimestampTz: true,
		IcebergUUID:        true,
		IcebergList:        true,
		IcebergMap:         true,
		IcebergStruct:      true,
	}

	return &DefaultIcebergTypeValidator{
		primitiveTypes: primitiveTypes,
	}
}

func (v *DefaultIcebergTypeValidator) IsValidType(typeStr string) bool {
	typeStr = strings.TrimSpace(typeStr)

	// Check primitive types first
	if v.primitiveTypes[typeStr] {
		return true
	}

	// Check decimal with precision/scale
	if strings.HasPrefix(typeStr, "decimal(") {
		return v.isValidDecimalType(typeStr)
	}

	// Check complex types
	if strings.HasPrefix(typeStr, "list<") {
		return v.isValidListType(typeStr)
	}

	if strings.HasPrefix(typeStr, "map<") {
		return v.isValidMapType(typeStr)
	}

	if strings.HasPrefix(typeStr, "struct<") {
		return v.isValidStructType(typeStr)
	}

	return false
}

func (v *DefaultIcebergTypeValidator) ParseType(typeStr string) (IcebergType, error) {
	typeStr = strings.TrimSpace(typeStr)

	// Parse primitive types
	if v.primitiveTypes[typeStr] {
		return &PrimitiveType{TypeName: typeStr}, nil
	}

	// Parse decimal with precision/scale
	if strings.HasPrefix(typeStr, "decimal(") {
		return v.parseDecimalType(typeStr)
	}

	// Parse complex types
	if strings.HasPrefix(typeStr, "list<") {
		return v.parseListType(typeStr)
	}

	if strings.HasPrefix(typeStr, "map<") {
		return v.parseMapType(typeStr)
	}

	if strings.HasPrefix(typeStr, "struct<") {
		return v.parseStructType(typeStr)
	}

	return nil, errors.New(ErrUnsupportedType, "unsupported type", nil).
		AddContext("type_string", typeStr)
}

func (v *DefaultIcebergTypeValidator) ValidateComplexType(typeStr string) error {
	parsedType, err := v.ParseType(typeStr)
	if err != nil {
		return err
	}

	return parsedType.Validate()
}

func (v *DefaultIcebergTypeValidator) GetSupportedTypes() []string {
	types := make([]string, 0, len(v.primitiveTypes))
	for typeName := range v.primitiveTypes {
		types = append(types, typeName)
	}
	return types
}

// Helper functions for type validation and parsing

func (v *DefaultIcebergTypeValidator) isValidDecimalType(typeStr string) bool {
	_, err := v.parseDecimalType(typeStr)
	return err == nil
}

func (v *DefaultIcebergTypeValidator) parseDecimalType(typeStr string) (*PrimitiveType, error) {
	// Match decimal(precision,scale) pattern
	re := regexp.MustCompile(`^decimal\((\d+),(\d+)\)$`)
	matches := re.FindStringSubmatch(typeStr)

	if len(matches) != 3 {
		return nil, errors.New(ErrTypeValidationFailed, "invalid decimal format", nil).
			AddContext("type_string", typeStr).
			AddContext("expected_format", "decimal(precision,scale)")
	}

	precision, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, errors.New(ErrInvalidDecimalPrecision, "invalid decimal precision", err).
			AddContext("precision_string", matches[1])
	}

	scale, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, errors.New(ErrInvalidDecimalScale, "invalid decimal scale", err).
			AddContext("scale_string", matches[2])
	}

	decimalType := &PrimitiveType{
		TypeName:  IcebergDecimal,
		Precision: precision,
		Scale:     scale,
	}

	if err := decimalType.Validate(); err != nil {
		return nil, err
	}

	return decimalType, nil
}

func (v *DefaultIcebergTypeValidator) isValidListType(typeStr string) bool {
	_, err := v.parseListType(typeStr)
	return err == nil
}

func (v *DefaultIcebergTypeValidator) parseListType(typeStr string) (*ListType, error) {
	if !strings.HasPrefix(typeStr, "list<") || !strings.HasSuffix(typeStr, ">") {
		return nil, errors.New(ErrTypeValidationFailed, "invalid list format", nil).
			AddContext("type_string", typeStr)
	}

	elementTypeStr := typeStr[5 : len(typeStr)-1] // Remove "list<" and ">"
	elementTypeStr = strings.TrimSpace(elementTypeStr)

	if elementTypeStr == "" {
		return nil, errors.New(ErrInvalidListElement, "list element type cannot be empty", nil)
	}

	elementType, err := v.ParseType(elementTypeStr)
	if err != nil {
		return nil, errors.New(ErrInvalidListElement, "invalid list element type", err).
			AddContext("element_type_string", elementTypeStr)
	}

	return &ListType{ElementType: elementType}, nil
}

func (v *DefaultIcebergTypeValidator) isValidMapType(typeStr string) bool {
	_, err := v.parseMapType(typeStr)
	return err == nil
}

func (v *DefaultIcebergTypeValidator) parseMapType(typeStr string) (*MapType, error) {
	if !strings.HasPrefix(typeStr, "map<") || !strings.HasSuffix(typeStr, ">") {
		return nil, errors.New(ErrTypeValidationFailed, "invalid map format", nil).
			AddContext("type_string", typeStr)
	}

	content := typeStr[4 : len(typeStr)-1] // Remove "map<" and ">"
	content = strings.TrimSpace(content)

	// Find the comma that separates key and value types
	// Need to handle nested types properly
	commaIndex := v.findTopLevelComma(content)
	if commaIndex == -1 {
		return nil, errors.New(ErrTypeValidationFailed, "map must have key and value types separated by comma", nil).
			AddContext("type_string", typeStr)
	}

	keyTypeStr := strings.TrimSpace(content[:commaIndex])
	valueTypeStr := strings.TrimSpace(content[commaIndex+1:])

	if keyTypeStr == "" {
		return nil, errors.New(ErrInvalidMapKeyType, "map key type cannot be empty", nil)
	}
	if valueTypeStr == "" {
		return nil, errors.New(ErrInvalidMapValueType, "map value type cannot be empty", nil)
	}

	keyType, err := v.ParseType(keyTypeStr)
	if err != nil {
		return nil, errors.New(ErrInvalidMapKeyType, "invalid map key type", err).
			AddContext("key_type_string", keyTypeStr)
	}

	valueType, err := v.ParseType(valueTypeStr)
	if err != nil {
		return nil, errors.New(ErrInvalidMapValueType, "invalid map value type", err).
			AddContext("value_type_string", valueTypeStr)
	}

	return &MapType{KeyType: keyType, ValueType: valueType}, nil
}

func (v *DefaultIcebergTypeValidator) isValidStructType(typeStr string) bool {
	_, err := v.parseStructType(typeStr)
	return err == nil
}

func (v *DefaultIcebergTypeValidator) parseStructType(typeStr string) (*StructType, error) {
	if !strings.HasPrefix(typeStr, "struct<") || !strings.HasSuffix(typeStr, ">") {
		return nil, errors.New(ErrTypeValidationFailed, "invalid struct format", nil).
			AddContext("type_string", typeStr)
	}

	content := typeStr[7 : len(typeStr)-1] // Remove "struct<" and ">"
	content = strings.TrimSpace(content)

	if content == "" {
		return nil, errors.New(ErrInvalidStructField, "struct must have at least one field", nil)
	}

	// Parse field definitions
	fieldStrs := v.splitTopLevelCommas(content)
	var fields []StructField

	for i, fieldStr := range fieldStrs {
		fieldStr = strings.TrimSpace(fieldStr)

		// Find the colon that separates field name and type
		colonIndex := strings.Index(fieldStr, ":")
		if colonIndex == -1 {
			return nil, errors.New(ErrInvalidStructField, "missing colon separator in struct field", nil).
				AddContext("field_index", i+1).
				AddContext("expected_format", "name:type")
		}

		fieldName := strings.TrimSpace(fieldStr[:colonIndex])
		fieldTypeStr := strings.TrimSpace(fieldStr[colonIndex+1:])

		if fieldName == "" {
			return nil, errors.New(ErrInvalidStructField, "struct field name cannot be empty", nil).
				AddContext("field_index", i+1)
		}
		if fieldTypeStr == "" {
			return nil, errors.New(ErrInvalidStructField, "struct field type cannot be empty", nil).
				AddContext("field_index", i+1).
				AddContext("field_name", fieldName)
		}

		fieldType, err := v.ParseType(fieldTypeStr)
		if err != nil {
			return nil, errors.New(ErrInvalidStructField, "invalid struct field type", err).
				AddContext("field_index", i+1).
				AddContext("field_name", fieldName).
				AddContext("field_type_string", fieldTypeStr)
		}

		fields = append(fields, StructField{
			Name: fieldName,
			Type: fieldType,
		})
	}

	return &StructType{Fields: fields}, nil
}

// Helper function to find top-level comma (not inside nested types)
func (v *DefaultIcebergTypeValidator) findTopLevelComma(s string) int {
	depth := 0
	for i, char := range s {
		switch char {
		case '<':
			depth++
		case '>':
			depth--
		case ',':
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// Helper function to split by top-level commas
func (v *DefaultIcebergTypeValidator) splitTopLevelCommas(s string) []string {
	var result []string
	var current strings.Builder
	depth := 0

	for _, char := range s {
		switch char {
		case '<':
			depth++
			current.WriteRune(char)
		case '>':
			depth--
			current.WriteRune(char)
		case ',':
			if depth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(char)
			}
		default:
			current.WriteRune(char)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

// Helper function to check if a type is a valid primitive type
func isValidPrimitiveType(typeName string) bool {
	primitiveTypes := map[string]bool{
		IcebergBoolean:     true,
		IcebergInt32:       true,
		IcebergInt64:       true,
		IcebergFloat32:     true,
		IcebergFloat64:     true,
		IcebergDecimal:     true,
		IcebergString:      true,
		IcebergBinary:      true,
		IcebergDate:        true,
		IcebergTime:        true,
		IcebergTimestamp:   true,
		IcebergTimestampTz: true,
		IcebergUUID:        true,
	}

	return primitiveTypes[typeName]
}

// TypeConverter provides conversion utilities between Iceberg types and registry formats
type TypeConverter interface {
	ConvertToRegistryFormat(icebergType IcebergType) (string, error)
	ConvertFromRegistryFormat(registryType string) (IcebergType, error)
}

// DefaultTypeConverter implements TypeConverter
type DefaultTypeConverter struct {
	validator IcebergTypeValidator
}

// NewTypeConverter creates a new type converter instance
func NewTypeConverter() TypeConverter {
	return &DefaultTypeConverter{
		validator: NewIcebergTypeValidator(),
	}
}

func (tc *DefaultTypeConverter) ConvertToRegistryFormat(icebergType IcebergType) (string, error) {
	if icebergType == nil {
		return "", errors.New(ErrTypeConversionFailed, "iceberg type cannot be nil", nil)
	}

	if err := icebergType.Validate(); err != nil {
		return "", errors.AddContext(err, "operation", "convert_to_registry_format")
	}

	// Registry format is the same as Iceberg type string representation
	return icebergType.String(), nil
}

func (tc *DefaultTypeConverter) ConvertFromRegistryFormat(registryType string) (IcebergType, error) {
	if registryType == "" {
		return nil, errors.New(ErrTypeConversionFailed, "registry type cannot be empty", nil)
	}

	// Parse the registry type string as an Iceberg type
	icebergType, err := tc.validator.ParseType(registryType)
	if err != nil {
		return nil, errors.AddContext(err, "registry_type", registryType).
			AddContext("operation", "convert_from_registry_format")
	}

	return icebergType, nil
}

// Utility functions for common type operations

// GetAllSupportedTypes returns all supported Iceberg types including examples of complex types
func GetAllSupportedTypes() []string {
	validator := NewIcebergTypeValidator()
	primitiveTypes := validator.GetSupportedTypes()

	// Add examples of complex types
	complexExamples := []string{
		"decimal(10,2)",
		"list<string>",
		"map<string,int32>",
		"struct<name:string,age:int32>",
		"list<struct<id:int64,name:string>>",
		"map<string,list<int32>>",
	}

	return append(primitiveTypes, complexExamples...)
}

// ValidateTypeString validates a type string and returns detailed error information
func ValidateTypeString(typeStr string) error {
	validator := NewIcebergTypeValidator()

	if !validator.IsValidType(typeStr) {
		supportedTypes := validator.GetSupportedTypes()
		return errors.New(ErrUnsupportedType, "invalid Iceberg type", nil).
			AddContext("type_string", typeStr).
			AddContext("supported_primitive_types", supportedTypes).
			AddSuggestion("Use supported primitive types or complex types like list<type>, map<keyType,valueType>, struct<field:type,...>, decimal(precision,scale)")
	}

	return validator.ValidateComplexType(typeStr)
}

// ParseAndValidateType parses and validates a type string, returning the parsed type
func ParseAndValidateType(typeStr string) (IcebergType, error) {
	validator := NewIcebergTypeValidator()

	parsedType, err := validator.ParseType(typeStr)
	if err != nil {
		return nil, err
	}

	if err := parsedType.Validate(); err != nil {
		return nil, errors.New(ErrTypeValidationFailed, "type validation failed", err)
	}

	return parsedType, nil
}

// GetTypeComplexity returns the nesting depth of a type (0 for primitives)
func GetTypeComplexity(icebergType IcebergType) int {
	if !icebergType.IsComplex() {
		return 0
	}

	maxNestedComplexity := 0
	for _, nestedType := range icebergType.GetNestedTypes() {
		nestedComplexity := GetTypeComplexity(nestedType)
		if nestedComplexity > maxNestedComplexity {
			maxNestedComplexity = nestedComplexity
		}
	}

	return maxNestedComplexity + 1
}

// IsCompatibleType checks if two Iceberg types are compatible for operations
func IsCompatibleType(type1, type2 IcebergType) bool {
	if type1 == nil || type2 == nil {
		return false
	}

	// Exact match
	if type1.String() == type2.String() {
		return true
	}

	// Numeric compatibility
	if isNumericType(type1) && isNumericType(type2) {
		return true
	}

	// String compatibility
	if isStringType(type1) && isStringType(type2) {
		return true
	}

	return false
}

// Helper functions for type checking

func isNumericType(icebergType IcebergType) bool {
	if icebergType.IsComplex() {
		return false
	}

	primitiveType, ok := icebergType.(*PrimitiveType)
	if !ok {
		return false
	}

	numericTypes := map[string]bool{
		IcebergInt32:   true,
		IcebergInt64:   true,
		IcebergFloat32: true,
		IcebergFloat64: true,
		IcebergDecimal: true,
	}

	return numericTypes[primitiveType.TypeName]
}

func isStringType(icebergType IcebergType) bool {
	if icebergType.IsComplex() {
		return false
	}

	primitiveType, ok := icebergType.(*PrimitiveType)
	if !ok {
		return false
	}

	return primitiveType.TypeName == IcebergString
}

// GetPrimitiveTypesOnly returns only the primitive Iceberg types
func GetPrimitiveTypesOnly() []string {
	return []string{
		IcebergBoolean,
		IcebergInt32,
		IcebergInt64,
		IcebergFloat32,
		IcebergFloat64,
		IcebergDecimal,
		IcebergString,
		IcebergBinary,
		IcebergDate,
		IcebergTime,
		IcebergTimestamp,
		IcebergTimestampTz,
		IcebergUUID,
	}
}

// GetComplexTypesOnly returns only the complex Iceberg type names
func GetComplexTypesOnly() []string {
	return []string{
		IcebergList,
		IcebergMap,
		IcebergStruct,
	}
}
