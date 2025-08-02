package catalog

import (
	"fmt"

	"github.com/TFMV/icebox/deprecated/catalog/json"
	"github.com/TFMV/icebox/deprecated/catalog/rest"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
)

// CatalogInterface defines the common interface for all catalog implementations
type CatalogInterface interface {
	icebergcatalog.Catalog
	Name() string
	Close() error
}

// NewCatalog creates a new catalog based on the configuration type
func NewCatalog(cfg *config.Config) (CatalogInterface, error) {
	switch cfg.Catalog.Type {
	case "sqlite":
		return sqlite.NewCatalog(cfg)
	case "rest":
		return rest.NewCatalog(cfg)
	case "json":
		return json.NewCatalog(cfg)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", cfg.Catalog.Type)
	}
}
