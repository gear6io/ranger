package rest

import (
	"context"
	"fmt"
	"iter"
	"net/url"

	"github.com/TFMV/icebox/server/config"
	"github.com/TFMV/icebox/server/paths"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergrest "github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// ComponentType defines the REST catalog component type identifier
const ComponentType = "catalog"

// Catalog implements the iceberg-go catalog.Catalog interface using a REST catalog
type Catalog struct {
	name        string
	restCatalog *icebergrest.Catalog
	fileIO      icebergio.IO
	pathManager paths.PathManager
	config      *config.Config
}

// NewCatalog creates a new REST catalog wrapper
func NewCatalog(cfg *config.Config, pathManager paths.PathManager) (*Catalog, error) {
	// Validate catalog type
	catalogType := cfg.GetCatalogType()
	if catalogType != "rest" {
		return nil, fmt.Errorf("expected catalog type 'rest', got '%s'", catalogType)
	}

	// Get catalog URI from path manager
	catalogURI := pathManager.GetCatalogURI("rest")
	if catalogURI == "" {
		return nil, fmt.Errorf("catalog URI is required for REST catalog")
	}

	// Parse the URI to get base URL
	baseURL, err := url.Parse(catalogURI)
	if err != nil {
		return nil, fmt.Errorf("invalid catalog URI: %w", err)
	}

	// Build options for the iceberg-go REST catalog
	var opts []icebergrest.Option

	// Note: Warehouse location is managed by the Storage package, not the catalog
	// The catalog only manages Iceberg catalog and metadata files

	// Create the REST catalog with required parameters
	restCatalog, err := icebergrest.NewCatalog(context.Background(), "icebox-rest-catalog", baseURL.String(), opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST catalog: %w", err)
	}

	return &Catalog{
		name:        "icebox-rest-catalog",
		restCatalog: restCatalog,
		fileIO:      icebergio.LocalFS{},
		pathManager: pathManager,
		config:      cfg,
	}, nil
}

// CatalogType returns the catalog type
func (c *Catalog) CatalogType() icebergcatalog.Type {
	return icebergcatalog.REST
}

// Name returns the catalog name
func (c *Catalog) Name() string {
	return c.name
}

// CreateTable creates a new table in the catalog
func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...icebergcatalog.CreateTableOpt) (*table.Table, error) {
	return c.restCatalog.CreateTable(ctx, identifier, schema, opts...)
}

// CommitTable commits table changes to the catalog
func (c *Catalog) CommitTable(ctx context.Context, tbl *table.Table, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	return c.restCatalog.CommitTable(ctx, tbl, reqs, updates)
}

// LoadTable loads a table from the catalog
func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	return c.restCatalog.LoadTable(ctx, identifier, props)
}

// DropTable drops a table from the catalog
func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	return c.restCatalog.DropTable(ctx, identifier)
}

// RenameTable renames a table in the catalog
func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	return c.restCatalog.RenameTable(ctx, from, to)
}

// CheckTableExists checks if a table exists in the catalog
func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	return c.restCatalog.CheckTableExists(ctx, identifier)
}

// ListTables lists all tables in a namespace
func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return c.restCatalog.ListTables(ctx, namespace)
}

// CreateNamespace creates a new namespace
func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	return c.restCatalog.CreateNamespace(ctx, namespace, props)
}

// DropNamespace drops a namespace from the catalog
func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	return c.restCatalog.DropNamespace(ctx, namespace)
}

// CheckNamespaceExists checks if a namespace exists
func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	return c.restCatalog.CheckNamespaceExists(ctx, namespace)
}

// LoadNamespaceProperties loads properties for a namespace
func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	return c.restCatalog.LoadNamespaceProperties(ctx, namespace)
}

// UpdateNamespaceProperties updates properties for a namespace
func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (icebergcatalog.PropertiesUpdateSummary, error) {
	return c.restCatalog.UpdateNamespaceProperties(ctx, namespace, removals, updates)
}

// ListNamespaces lists all namespaces
func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	return c.restCatalog.ListNamespaces(ctx, parent)
}

// Close cleans up any resources used by the catalog
func (c *Catalog) Close() error {
	// The iceberg-go REST catalog doesn't have a Close method, so this is a no-op
	return nil
}

// GetType returns the component type identifier
func (c *Catalog) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the REST catalog
func (c *Catalog) Shutdown(ctx context.Context) error {
	// The iceberg-go REST catalog doesn't have a Close method, so this is a no-op
	return nil
}
