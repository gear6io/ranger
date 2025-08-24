package catalog

import (
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/catalog/json"
	"github.com/gear6io/ranger/server/catalog/rest"
	"github.com/gear6io/ranger/server/catalog/sqlite"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/paths"
	"github.com/gear6io/ranger/server/shared"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
)

// CatalogInterface defines the common interface for all catalog implementations
type CatalogInterface interface {
	icebergcatalog.Catalog
	shared.Component
	Name() string
	Close() error
}

// NewCatalog creates a new catalog based on the configuration and path manager
func NewCatalog(cfg *config.Config, pathManager paths.PathManager) (CatalogInterface, error) {
	catalogType := cfg.GetCatalogType()

	switch catalogType {
	case "sqlite":
		return sqlite.NewCatalog(cfg, pathManager)
	case "rest":
		return rest.NewCatalog(cfg, pathManager)
	case "json":
		return json.NewCatalog(cfg, pathManager)
	default:
		return nil, errors.New(ErrUnsupportedCatalogType, "unsupported catalog type", nil).AddContext("catalog_type", catalogType)
	}
}
