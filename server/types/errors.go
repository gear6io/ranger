package types

import (
	"github.com/gear6io/ranger/pkg/errors"
)

// Type validation error codes
var (
	ErrInvalidPrimitiveType         = errors.MustNewCode("types.invalid_primitive_type")
	ErrInvalidDecimalPrecision      = errors.MustNewCode("types.invalid_decimal_precision")
	ErrInvalidDecimalScale          = errors.MustNewCode("types.invalid_decimal_scale")
	ErrDecimalScaleExceedsPrecision = errors.MustNewCode("types.decimal_scale_exceeds_precision")

	ErrInvalidListElement   = errors.MustNewCode("types.invalid_list_element")
	ErrInvalidMapKeyType    = errors.MustNewCode("types.invalid_map_key_type")
	ErrInvalidMapValueType  = errors.MustNewCode("types.invalid_map_value_type")
	ErrInvalidStructField   = errors.MustNewCode("types.invalid_struct_field")
	ErrTypeValidationFailed = errors.MustNewCode("types.validation_failed")
	ErrTypeConversionFailed = errors.MustNewCode("types.conversion_failed")
	ErrUnsupportedType      = errors.MustNewCode("types.unsupported_type")
)
