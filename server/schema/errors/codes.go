package errors

import "github.com/gear6io/ranger/pkg/errors"

// Schema package error codes
var (
	// Schema validation errors
	SchemaValidationFailed     = errors.MustNewCode("schema.validation.failed")
	SchemaInvalidFieldID       = errors.MustNewCode("schema.validation.invalid_field_id")
	SchemaDuplicateFieldName   = errors.MustNewCode("schema.validation.duplicate_field_name")
	SchemaInvalidType          = errors.MustNewCode("schema.validation.invalid_type")
	SchemaInvalidNestedType    = errors.MustNewCode("schema.validation.invalid_nested_type")
	SchemaInvalidDecimalParams = errors.MustNewCode("schema.validation.invalid_decimal_params")
	SchemaInvalidFixedLength   = errors.MustNewCode("schema.validation.invalid_fixed_length")

	// Schema evolution errors
	SchemaEvolutionFailed         = errors.MustNewCode("schema.evolution.failed")
	SchemaIncompatibleChange      = errors.MustNewCode("schema.evolution.incompatible_change")
	SchemaFieldRemovalNotAllowed  = errors.MustNewCode("schema.evolution.field_removal_not_allowed")
	SchemaTypeNarrowingNotAllowed = errors.MustNewCode("schema.evolution.type_narrowing_not_allowed")
	SchemaRequiredFieldChange     = errors.MustNewCode("schema.evolution.required_field_change")
	SchemaFieldIDConflict         = errors.MustNewCode("schema.evolution.field_id_conflict")

	// Type conversion errors
	TypeConversionFailed      = errors.MustNewCode("schema.conversion.failed")
	TypeUnsupportedConversion = errors.MustNewCode("schema.conversion.unsupported")
	TypeIncompatibleMapping   = errors.MustNewCode("schema.conversion.incompatible_mapping")
	TypeParameterMismatch     = errors.MustNewCode("schema.conversion.parameter_mismatch")

	// Data validation errors
	DataValidationFailed    = errors.MustNewCode("schema.data_validation.failed")
	DataTypeMismatch        = errors.MustNewCode("schema.data_validation.type_mismatch")
	DataNullValueNotAllowed = errors.MustNewCode("schema.data_validation.null_value_not_allowed")
	DataValueOutOfRange     = errors.MustNewCode("schema.data_validation.value_out_of_range")
	DataInvalidFormat       = errors.MustNewCode("schema.data_validation.invalid_format")
	DataInvalidUUID         = errors.MustNewCode("schema.data_validation.invalid_uuid")
	DataInvalidDecimal      = errors.MustNewCode("schema.data_validation.invalid_decimal")
	DataInvalidTimestamp    = errors.MustNewCode("schema.data_validation.invalid_timestamp")

	// Registry errors
	RegistryOperationFailed       = errors.MustNewCode("schema.registry.operation_failed")
	RegistrySchemaNotFound        = errors.MustNewCode("schema.registry.schema_not_found")
	RegistryVersionNotFound       = errors.MustNewCode("schema.registry.version_not_found")
	RegistrySerializationFailed   = errors.MustNewCode("schema.registry.serialization_failed")
	RegistryDeserializationFailed = errors.MustNewCode("schema.registry.deserialization_failed")

	// Parser integration errors
	ParserTypeResolutionFailed = errors.MustNewCode("schema.parser.type_resolution_failed")
	ParserTypeInferenceFailed  = errors.MustNewCode("schema.parser.type_inference_failed")
	ParserUnsupportedType      = errors.MustNewCode("schema.parser.unsupported_type")
	ParserTypeCastFailed       = errors.MustNewCode("schema.parser.type_cast_failed")

	// SQL syntax errors
	InvalidSQLSyntax = errors.MustNewCode("schema.sql.invalid_syntax")
)
