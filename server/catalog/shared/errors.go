package shared

import (
	"github.com/TFMV/icebox/pkg/errors"
)

// Catalog-specific error codes
var (
	CatalogNotFound       = errors.MustNewCode("catalog.not_found")
	CatalogAlreadyExists  = errors.MustNewCode("catalog.already_exists")
	CatalogValidation     = errors.MustNewCode("catalog.validation")
	CatalogConcurrentMod  = errors.MustNewCode("catalog.concurrent_modification")
	CatalogUnsupported    = errors.MustNewCode("catalog.unsupported")
	CatalogInvalidInput   = errors.MustNewCode("catalog.invalid_input")
	CatalogInternal       = errors.MustNewCode("catalog.internal")
	CatalogNamespaceEmpty = errors.MustNewCode("catalog.namespace_empty")
	CatalogTableNotFound  = errors.MustNewCode("catalog.table_not_found")
	CatalogViewNotFound   = errors.MustNewCode("catalog.view_not_found")
)

// Helper functions for common catalog errors
func NewCatalogNotFound(message string) *errors.Error {
	return errors.New(CatalogNotFound, message)
}

func NewCatalogConcurrentModification(message string) *errors.Error {
	return errors.New(CatalogConcurrentMod, message)
}

func NewCatalogAlreadyExists(message string) *errors.Error {
	return errors.New(CatalogAlreadyExists, message)
}

func NewCatalogValidation(field, message string) *errors.Error {
	err := errors.New(CatalogValidation, message)
	err.AddContext("field", field)
	return err
}

func NewCatalogUnsupported(message string) *errors.Error {
	return errors.New(CatalogUnsupported, message)
}

func NewCatalogInvalidInput(field, message string) *errors.Error {
	err := errors.New(CatalogInvalidInput, message)
	err.AddContext("field", field)
	return err
}

func NewCatalogInternal(message string) *errors.Error {
	return errors.New(CatalogInternal, message)
}

func NewCatalogNamespaceEmpty(message string) *errors.Error {
	return errors.New(CatalogNamespaceEmpty, message)
}

func NewCatalogTableNotFound(message string) *errors.Error {
	return errors.New(CatalogTableNotFound, message)
}

func NewCatalogViewNotFound(message string) *errors.Error {
	return errors.New(CatalogViewNotFound, message)
}
