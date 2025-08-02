package rest

import (
	"context"
	"crypto/tls"
	"fmt"
	"iter"
	"net/url"

	"github.com/TFMV/icebox/deprecated/config"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	icebergrest "github.com/apache/iceberg-go/catalog/rest"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
)

// Catalog implements the iceberg-go catalog.Catalog interface using a REST catalog
type Catalog struct {
	name        string
	restCatalog *icebergrest.Catalog
	fileIO      icebergio.IO
	config      *config.Config
}

// NewCatalog creates a new REST catalog wrapper
func NewCatalog(cfg *config.Config) (*Catalog, error) {
	if cfg.Catalog.REST == nil {
		return nil, fmt.Errorf("REST catalog configuration is required")
	}

	restConfig := cfg.Catalog.REST

	// Build options for the iceberg-go REST catalog
	var opts []icebergrest.Option

	// Add OAuth configuration if present
	if restConfig.OAuth != nil {
		if restConfig.OAuth.Token != "" {
			opts = append(opts, icebergrest.WithOAuthToken(restConfig.OAuth.Token))
		}
		if restConfig.OAuth.Credential != "" {
			opts = append(opts, icebergrest.WithCredential(restConfig.OAuth.Credential))
		}
		if restConfig.OAuth.AuthURL != "" {
			authURL, err := url.Parse(restConfig.OAuth.AuthURL)
			if err != nil {
				return nil, fmt.Errorf("invalid auth URL: %w", err)
			}
			opts = append(opts, icebergrest.WithAuthURI(authURL))
		}
		if restConfig.OAuth.Scope != "" {
			opts = append(opts, icebergrest.WithScope(restConfig.OAuth.Scope))
		}
	}

	// Add SigV4 configuration if present
	if restConfig.SigV4 != nil && restConfig.SigV4.Enabled {
		if restConfig.SigV4.Region != "" && restConfig.SigV4.Service != "" {
			opts = append(opts, icebergrest.WithSigV4RegionSvc(restConfig.SigV4.Region, restConfig.SigV4.Service))
		} else {
			opts = append(opts, icebergrest.WithSigV4())
		}
	}

	// Add TLS configuration if present
	if restConfig.TLS != nil {
		tlsConfig := &tls.Config{
			InsecureSkipVerify: restConfig.TLS.SkipVerify,
		}
		opts = append(opts, icebergrest.WithTLSConfig(tlsConfig))
	}

	// Add warehouse location if present
	if restConfig.WarehouseLocation != "" {
		opts = append(opts, icebergrest.WithWarehouseLocation(restConfig.WarehouseLocation))
	}

	// Add metadata location if present
	if restConfig.MetadataLocation != "" {
		opts = append(opts, icebergrest.WithMetadataLocation(restConfig.MetadataLocation))
	}

	// Add prefix if present
	if restConfig.Prefix != "" {
		opts = append(opts, icebergrest.WithPrefix(restConfig.Prefix))
	}

	// Add additional properties if present
	if len(restConfig.AdditionalProps) > 0 {
		props := make(iceberg.Properties)
		for k, v := range restConfig.AdditionalProps {
			props[k] = v
		}
		opts = append(opts, icebergrest.WithAdditionalProps(props))
	}

	// Add credentials as additional properties
	if len(restConfig.Credentials) > 0 {
		props := make(iceberg.Properties)
		for k, v := range restConfig.Credentials {
			props[k] = v
		}
		opts = append(opts, icebergrest.WithAdditionalProps(props))
	}

	// Create the underlying REST catalog
	ctx := context.Background()
	restCatalog, err := icebergrest.NewCatalog(ctx, cfg.Name, restConfig.URI, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create REST catalog: %w", err)
	}

	// Create appropriate FileIO based on storage configuration
	var fileIO icebergio.IO
	switch cfg.Storage.Type {
	case "fs":
		fileIO = icebergio.LocalFS{}
	default:
		// Use the default FileIO provided by iceberg-go
		fileIO = icebergio.LocalFS{}
	}

	return &Catalog{
		name:        cfg.Name,
		restCatalog: restCatalog,
		fileIO:      fileIO,
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
