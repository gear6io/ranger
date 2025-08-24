package catalog

import "github.com/gear6io/ranger/pkg/errors"

// Catalog-specific error codes
var (
	ErrUnsupportedCatalogType = errors.MustNewCode("catalog.unsupported_type")
)
