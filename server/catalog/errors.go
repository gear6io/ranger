package catalog

import "github.com/TFMV/icebox/pkg/errors"

// Catalog-specific error codes
var (
	ErrUnsupportedCatalogType = errors.MustNewCode("catalog.unsupported_type")
)
