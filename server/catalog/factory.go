package catalog

import (
	"fmt"

	"github.com/TFMV/icebox/server/catalog/json"
	"github.com/TFMV/icebox/server/catalog/rest"
	"github.com/TFMV/icebox/server/catalog/sqlite"
	"github.com/TFMV/icebox/server/config"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
)

// CatalogInterface defines the common interface for all catalog implementations
type CatalogInterface interface {
	icebergcatalog.Catalog
	Name() string
	Close() error
}

// NewCatalog creates a new catalog based on the configuration
func NewCatalog(cfg *config.Config) (CatalogInterface, error) {
	catalogType := cfg.GetCatalogType()

	switch catalogType {
	case "sqlite":
		return sqlite.NewCatalog(cfg)
	case "rest":
		return rest.NewCatalog(cfg)
	case "json":
		return json.NewCatalog(cfg)
	default:
		return nil, fmt.Errorf("unsupported catalog type: %s", catalogType)
	}
}
