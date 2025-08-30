package types

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
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
		return fmt.Errorf("invalid primitive type: %s", pt.TypeName)
	}

	if pt.TypeName == IcebergDecimal {
		if pt.Precision <= 0 {
			return fmt.Errorf("decimal precision must be positive, got: %d", pt.Precision)
		}
		if pt.Scale < 0 {
			return fmt.Errorf("decimal scale must be non-negative, got: %d", pt.Scale)
		}
		if pt.Scale > pt.Precision {
			return fmt.Errorf("decimal scale (%d) cannot exceed precision (%d)", pt.Scale, pt.Precision)
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
		return fmt.Errorf("list element type cannot be nil")
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
		return fmt.Errorf("map key type cannot be nil")
	}
	if mt.ValueType == nil {
		return fmt.Errorf("map value type cannot be nil")
	}

	if err := mt.KeyType.Validate(); err != nil {
		return fmt.Errorf("invalid map key type: %w", err)
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
		return fmt.Errorf("struct must have at least one field")
	}

	fieldNames := make(map[string]bool)
	for i, field := range st.Fields {
		if field.Name == "" {
			return fmt.Errorf("struct field %d: name cannot be empty", i)
		}

		if fieldNames[field.Name] {
			return fmt.Errorf("duplicate struct field name: %s", field.Name)
		}
		fieldNames[field.Name] = true

		if field.Type == nil {
			return fmt.Errorf("struct field '%s': type cannot be nil", field.Name)
		}

		if err := field.Type.Validate(); err != nil {
			return fmt.Errorf("struct field '%s': %w", field.Name, err)
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

	return nil, fmt.Errorf("unsupported type: %s", typeStr)
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
		return nil, fmt.Errorf("invalid decimal format: %s (expected: decimal(precision,scale))", typeStr)
	}

	precision, err := strconv.Atoi(matches[1])
	if err != nil {
		return nil, fmt.Errorf("invalid decimal precision: %s", matches[1])
	}

	scale, err := strconv.Atoi(matches[2])
	if err != nil {
		return nil, fmt.Errorf("invalid decimal scale: %s", matches[2])
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
		return nil, fmt.Errorf("invalid list format: %s", typeStr)
	}

	elementTypeStr := typeStr[5 : len(typeStr)-1] // Remove "list<" and ">"
	elementTypeStr = strings.TrimSpace(elementTypeStr)

	if elementTypeStr == "" {
		return nil, fmt.Errorf("list element type cannot be empty")
	}

	elementType, err := v.ParseType(elementTypeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid list element type: %w", err)
	}

	return &ListType{ElementType: elementType}, nil
}

func (v *DefaultIcebergTypeValidator) isValidMapType(typeStr string) bool {
	_, err := v.parseMapType(typeStr)
	return err == nil
}

func (v *DefaultIcebergTypeValidator) parseMapType(typeStr string) (*MapType, error) {
	if !strings.HasPrefix(typeStr, "map<") || !strings.HasSuffix(typeStr, ">") {
		return nil, fmt.Errorf("invalid map format: %s", typeStr)
	}

	content := typeStr[4 : len(typeStr)-1] // Remove "map<" and ">"
	content = strings.TrimSpace(content)

	// Find the comma that separates key and value types
	// Need to handle nested types properly
	commaIndex := v.findTopLevelComma(content)
	if commaIndex == -1 {
		return nil, fmt.Errorf("map must have key and value types separated by comma: %s", typeStr)
	}

	keyTypeStr := strings.TrimSpace(content[:commaIndex])
	valueTypeStr := strings.TrimSpace(content[commaIndex+1:])

	if keyTypeStr == "" {
		return nil, fmt.Errorf("map key type cannot be empty")
	}
	if valueTypeStr == "" {
		return nil, fmt.Errorf("map value type cannot be empty")
	}

	keyType, err := v.ParseType(keyTypeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid map key type: %w", err)
	}

	valueType, err := v.ParseType(valueTypeStr)
	if err != nil {
		return nil, fmt.Errorf("invalid map value type: %w", err)
	}

	return &MapType{KeyType: keyType, ValueType: valueType}, nil
}

func (v *DefaultIcebergTypeValidator) isValidStructType(typeStr string) bool {
	_, err := v.parseStructType(typeStr)
	return err == nil
}

func (v *DefaultIcebergTypeValidator) parseStructType(typeStr string) (*StructType, error) {
	if !strings.HasPrefix(typeStr, "struct<") || !strings.HasSuffix(typeStr, ">") {
		return nil, fmt.Errorf("invalid struct format: %s", typeStr)
	}

	content := typeStr[7 : len(typeStr)-1] // Remove "struct<" and ">"
	content = strings.TrimSpace(content)

	if content == "" {
		return nil, fmt.Errorf("struct must have at least one field")
	}

	// Parse field definitions
	fieldStrs := v.splitTopLevelCommas(content)
	var fields []StructField

	for i, fieldStr := range fieldStrs {
		fieldStr = strings.TrimSpace(fieldStr)

		// Find the colon that separates field name and type
		colonIndex := strings.Index(fieldStr, ":")
		if colonIndex == -1 {
			return nil, fmt.Errorf("struct field %d: missing colon separator (expected format: name:type)", i+1)
		}

		fieldName := strings.TrimSpace(fieldStr[:colonIndex])
		fieldTypeStr := strings.TrimSpace(fieldStr[colonIndex+1:])

		if fieldName == "" {
			return nil, fmt.Errorf("struct field %d: name cannot be empty", i+1)
		}
		if fieldTypeStr == "" {
			return nil, fmt.Errorf("struct field %d (%s): type cannot be empty", i+1, fieldName)
		}

		fieldType, err := v.ParseType(fieldTypeStr)
		if err != nil {
			return nil, fmt.Errorf("struct field %d (%s): invalid type: %w", i+1, fieldName, err)
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
		return "", fmt.Errorf("iceberg type cannot be nil")
	}

	if err := icebergType.Validate(); err != nil {
		return "", fmt.Errorf("invalid iceberg type: %w", err)
	}

	// Registry format is the same as Iceberg type string representation
	return icebergType.String(), nil
}

func (tc *DefaultTypeConverter) ConvertFromRegistryFormat(registryType string) (IcebergType, error) {
	if registryType == "" {
		return nil, fmt.Errorf("registry type cannot be empty")
	}

	// Parse the registry type string as an Iceberg type
	icebergType, err := tc.validator.ParseType(registryType)
	if err != nil {
		return nil, fmt.Errorf("failed to parse registry type '%s': %w", registryType, err)
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
		return fmt.Errorf("invalid Iceberg type '%s'. Supported primitive types: %v. Complex types: list<type>, map<keyType,valueType>, struct<field:type,...>, decimal(precision,scale)",
			typeStr, supportedTypes)
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
		return nil, fmt.Errorf("type validation failed: %w", err)
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
