package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/catalog"
	icebergio "github.com/apache/iceberg-go/io"
	"github.com/apache/iceberg-go/table"
	"github.com/gear6io/ranger/pkg/errors"
	catalogshared "github.com/gear6io/ranger/server/catalog/shared"
	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/paths"
	_ "github.com/mattn/go-sqlite3"
)

// ComponentType defines the SQLite catalog component type identifier
const ComponentType = "catalog"

// Catalog implements the iceberg-go catalog.Catalog interface using SQLite
type Catalog struct {
	name        string
	dbPath      string
	db          *sql.DB
	fileIO      icebergio.IO
	pathManager paths.PathManager
	config      *config.Config
}

// NewCatalog creates a new SQLite-based catalog
func NewCatalog(cfg *config.Config, pathManager paths.PathManager) (*Catalog, error) {
	// Validate catalog type
	catalogType := cfg.GetCatalogType()
	if catalogType != "sqlite" {
		return nil, catalogshared.NewCatalogValidation("catalog_type", fmt.Sprintf("expected catalog type 'sqlite', got '%s'", catalogType))
	}

	// Get catalog URI from path manager
	catalogURI := pathManager.GetCatalogURI("sqlite")
	if catalogURI == "" {
		return nil, catalogshared.NewCatalogValidation("catalog_uri", "catalog URI is required for SQLite catalog")
	}

	// Ensure the directory exists
	if err := os.MkdirAll(filepath.Dir(catalogURI), 0755); err != nil {
		return nil, errors.New(ErrCatalogDirectoryCreateFailed, "failed to create catalog directory", err)
	}

	db, err := sql.Open("sqlite3", catalogURI+"?_foreign_keys=on")
	if err != nil {
		return nil, errors.New(ErrDatabaseOpenFailed, "failed to open SQLite database", err)
	}

	// Create a local FileIO implementation for iceberg-go compatibility
	fileIO := icebergio.LocalFS{}

	return NewCatalogWithIO("ranger-sqlite-catalog", catalogURI, db, fileIO, pathManager, cfg)
}

// NewCatalogWithIO creates a new SQLite-based catalog with custom file IO
func NewCatalogWithIO(name, dbPath string, db *sql.DB, fileIO icebergio.IO, pathManager paths.PathManager, cfg *config.Config) (*Catalog, error) {
	cat := &Catalog{
		name:        name,
		dbPath:      dbPath,
		db:          db,
		fileIO:      fileIO,
		pathManager: pathManager,
		config:      cfg,
	}

	if err := cat.initializeDatabase(); err != nil {
		db.Close()
		return nil, errors.New(ErrDatabaseInitFailed, "failed to initialize database", err)
	}

	return cat, nil
}

// CatalogType returns the catalog type
func (c *Catalog) CatalogType() catalog.Type {
	return catalog.SQL
}

// Name returns the catalog name
func (c *Catalog) Name() string {
	return c.name
}

// Close closes the database connection
func (c *Catalog) Close() error {
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			return errors.New(ErrDatabaseCloseFailed, "failed to close SQLite catalog", err)
		}
	}
	return nil
}

// GetType returns the component type identifier
func (c *Catalog) GetType() string {
	return ComponentType
}

// Shutdown gracefully shuts down the SQLite catalog
func (c *Catalog) Shutdown(ctx context.Context) error {
	log.Printf("Shutting down SQLite catalog")

	// Close catalog
	if err := c.Close(); err != nil {
		return errors.New(ErrDatabaseCloseFailed, "failed to close SQLite catalog", err)
	}

	log.Printf("SQLite catalog shut down successfully")
	return nil
}

// initializeDatabase creates the necessary tables if they don't exist
func (c *Catalog) initializeDatabase() error {
	// Create iceberg_tables table
	createTablesSQL := `
	CREATE TABLE IF NOT EXISTS iceberg_tables (
		catalog_name TEXT NOT NULL,
		table_namespace TEXT NOT NULL,
		table_name TEXT NOT NULL,
		metadata_location TEXT,
		previous_metadata_location TEXT,
		PRIMARY KEY (catalog_name, table_namespace, table_name)
	)`

	if _, err := c.db.Exec(createTablesSQL); err != nil {
		return errors.New(ErrTableCreateFailed, "failed to create iceberg_tables table", err)
	}

	// Create iceberg_namespace_properties table
	createNamespacePropsSQL := `
	CREATE TABLE IF NOT EXISTS iceberg_namespace_properties (
		catalog_name TEXT NOT NULL,
		namespace TEXT NOT NULL,
		property_key TEXT NOT NULL,
		property_value TEXT,
		PRIMARY KEY (catalog_name, namespace, property_key)
	)`

	if _, err := c.db.Exec(createNamespacePropsSQL); err != nil {
		return errors.New(ErrTableCreateFailed, "failed to create iceberg_namespace_properties table", err)
	}

	return nil
}

// CreateTable creates a new table in the catalog
func (c *Catalog) CreateTable(ctx context.Context, identifier table.Identifier, schema *iceberg.Schema, opts ...catalog.CreateTableOpt) (*table.Table, error) {
	if len(identifier) == 0 {
		return nil, catalogshared.NewCatalogValidation("table_identifier", "table identifier cannot be empty")
	}

	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)

	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return nil, errors.New(ErrNamespaceCheckFailed, "failed to check namespace existence", err)
	}
	if !exists {
		return nil, catalog.ErrNoSuchNamespace
	}

	// Check if table already exists
	tableExists, err := c.CheckTableExists(ctx, identifier)
	if err != nil {
		return nil, errors.New(ErrTableCheckFailed, "failed to check table existence", err)
	}
	if tableExists {
		return nil, catalog.ErrTableAlreadyExists
	}

	// Parse options using proper iceberg-go APIs
	location := c.defaultTableLocation(identifier)
	properties := make(iceberg.Properties)

	// Set default properties
	properties["format-version"] = "2"
	properties["created-by"] = "ranger-catalog"

	// Apply create table options
	// Note: The current iceberg-go API doesn't expose option application directly
	// For now, we acknowledge the options but use default behavior
	for _, opt := range opts {
		// Options would be applied here when the API supports it
		_ = opt // Acknowledge the option for future enhancement
	}

	// Create proper table metadata using iceberg-go APIs
	metadata, err := table.NewMetadata(schema, iceberg.UnpartitionedSpec, table.UnsortedSortOrder, location, properties)
	if err != nil {
		return nil, errors.New(ErrMetadataBuilderFailed, "failed to create table metadata", err)
	}

	// Generate metadata location
	metadataLocation := c.newMetadataLocation(identifier, 1)

	// Write metadata to storage using proper APIs
	if err := c.writeMetadataFile(metadata, metadataLocation); err != nil {
		return nil, errors.New(ErrTableMetadataWriteFailed, "failed to write table metadata", err)
	}

	// Insert into database
	insertSQL := `
	INSERT INTO iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)
	VALUES (?, ?, ?, ?, ?)`

	namespaceStr := namespaceToString(namespace)
	_, err = c.db.ExecContext(ctx, insertSQL, c.name, namespaceStr, tableName, metadataLocation, nil)
	if err != nil {
		return nil, errors.New(ErrTableInsertFailed, "failed to insert table record", err)
	}

	// Load and return the table
	return c.LoadTable(ctx, identifier, properties)
}

// CommitTable commits table changes to the catalog
func (c *Catalog) CommitTable(ctx context.Context, tbl *table.Table, reqs []table.Requirement, updates []table.Update) (table.Metadata, string, error) {
	// Get current metadata location
	identifier := tbl.Identifier()
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	var currentMetadataLocation sql.NullString
	query := `SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr, tableName).Scan(&currentMetadataLocation)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, "", catalog.ErrNoSuchTable
		}
		return nil, "", errors.New(ErrMetadataQueryFailed, "failed to query current metadata", err)
	}

	// Validate requirements against current metadata
	currentMetadata := tbl.Metadata()
	for _, req := range reqs {
		if err := req.Validate(currentMetadata); err != nil {
			return nil, "", errors.New(ErrMetadataValidationFailed, "requirement validation failed", err)
		}
	}

	// Apply updates to create new metadata
	metadataBuilder, err := table.MetadataBuilderFromBase(currentMetadata)
	if err != nil {
		return nil, "", errors.New(ErrMetadataBuilderFailed, "failed to create metadata builder", err)
	}

	// Apply each update to the metadata builder
	for _, update := range updates {
		if err := update.Apply(metadataBuilder); err != nil {
			return nil, "", errors.New(ErrMetadataUpdateFailed, fmt.Sprintf("failed to apply update %s", update.Action()), err)
		}
	}

	// Build the new metadata
	newMetadata, err := metadataBuilder.Build()
	if err != nil {
		return nil, "", errors.New(ErrMetadataBuildFailed, "failed to build new metadata", err)
	}

	// Determine the new metadata version and location
	newVersion := c.getNextMetadataVersion(currentMetadataLocation.String)
	newMetadataLocation := c.newMetadataLocation(identifier, newVersion)

	// Write the new metadata file
	if err := c.writeMetadataFile(newMetadata, newMetadataLocation); err != nil {
		return nil, "", errors.New(ErrMetadataWriteFailed, "failed to write metadata file", err)
	}

	// Update database with the new metadata location
	updateSQL := `UPDATE iceberg_tables SET metadata_location = ?, previous_metadata_location = ? WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	_, err = c.db.ExecContext(ctx, updateSQL, newMetadataLocation, currentMetadataLocation.String, c.name, namespaceStr, tableName)
	if err != nil {
		return nil, "", errors.New(ErrMetadataLocationUpdateFailed, "failed to update table metadata location", err)
	}

	return newMetadata, newMetadataLocation, nil
}

// LoadTable loads a table from the catalog
func (c *Catalog) LoadTable(ctx context.Context, identifier table.Identifier, props iceberg.Properties) (*table.Table, error) {
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	var metadataLocation sql.NullString
	query := `SELECT metadata_location FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`

	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr, tableName).Scan(&metadataLocation)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, catalog.ErrNoSuchTable
		}
		return nil, errors.New(ErrTableQueryFailed, "failed to query table", err)
	}

	if !metadataLocation.Valid {
		return nil, catalogshared.NewCatalogValidation("table_metadata_location", "table metadata location is null")
	}

	// Load table using iceberg-go APIs
	tbl, err := table.NewFromLocation(identifier, metadataLocation.String, c.fileIO, c)
	if err != nil {
		return nil, errors.New(ErrTableLoadFailed, "failed to load table", err)
	}

	return tbl, nil
}

// DropTable drops a table from the catalog
func (c *Catalog) DropTable(ctx context.Context, identifier table.Identifier) error {
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	// Check if table exists
	exists, err := c.CheckTableExists(ctx, identifier)
	if err != nil {
		return errors.New(ErrTableCheckFailed, "failed to check table existence", err)
	}
	if !exists {
		return catalog.ErrNoSuchTable
	}

	// Delete from database
	deleteSQL := `DELETE FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	result, err := c.db.ExecContext(ctx, deleteSQL, c.name, namespaceStr, tableName)
	if err != nil {
		return errors.New(ErrTableDeleteFailed, "failed to delete table record", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return errors.New(ErrTableDeleteFailed, "failed to get rows affected", err)
	}
	if rowsAffected == 0 {
		return catalog.ErrNoSuchTable
	}

	return nil
}

// RenameTable renames a table in the catalog
func (c *Catalog) RenameTable(ctx context.Context, from, to table.Identifier) (*table.Table, error) {
	// Check if source table exists
	sourceTable, err := c.LoadTable(ctx, from, nil)
	if err != nil {
		return nil, errors.New(ErrTableLoadFailed, "failed to load source table", err)
	}

	// Check if destination namespace exists
	destNamespace := catalog.NamespaceFromIdent(to)
	nsExists, err := c.CheckNamespaceExists(ctx, destNamespace)
	if err != nil {
		return nil, errors.New(ErrNamespaceCheckFailed, "failed to check destination namespace", err)
	}
	if !nsExists {
		return nil, catalog.ErrNoSuchNamespace
	}

	// Check if destination table already exists
	destExists, err := c.CheckTableExists(ctx, to)
	if err != nil {
		return nil, errors.New(ErrTableCheckFailed, "failed to check destination table", err)
	}
	if destExists {
		return nil, catalog.ErrTableAlreadyExists
	}

	// Update the database record
	fromNamespace := catalog.NamespaceFromIdent(from)
	fromTableName := catalog.TableNameFromIdent(from)
	fromNamespaceStr := namespaceToString(fromNamespace)

	toNamespace := catalog.NamespaceFromIdent(to)
	toTableName := catalog.TableNameFromIdent(to)
	toNamespaceStr := namespaceToString(toNamespace)

	updateSQL := `UPDATE iceberg_tables SET table_namespace = ?, table_name = ? WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	_, err = c.db.ExecContext(ctx, updateSQL, toNamespaceStr, toTableName, c.name, fromNamespaceStr, fromTableName)
	if err != nil {
		return nil, errors.New(ErrTableRenameFailed, "failed to rename table in database", err)
	}

	// Return the renamed table
	newTable := table.New(to, sourceTable.Metadata(), sourceTable.MetadataLocation(), c.fileIO, c)
	return newTable, nil
}

// CheckTableExists checks if a table exists in the catalog
func (c *Catalog) CheckTableExists(ctx context.Context, identifier table.Identifier) (bool, error) {
	namespace := catalog.NamespaceFromIdent(identifier)
	tableName := catalog.TableNameFromIdent(identifier)
	namespaceStr := namespaceToString(namespace)

	var count int
	query := `SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ? AND table_name = ?`
	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr, tableName).Scan(&count)
	if err != nil {
		return false, errors.New(ErrTableCheckFailed, "failed to check table existence", err)
	}

	return count > 0, nil
}

// ListTables lists all tables in a namespace
func (c *Catalog) ListTables(ctx context.Context, namespace table.Identifier) iter.Seq2[table.Identifier, error] {
	return func(yield func(table.Identifier, error) bool) {
		namespaceStr := namespaceToString(namespace)
		query := `SELECT table_namespace, table_name FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ?`

		rows, err := c.db.QueryContext(ctx, query, c.name, namespaceStr)
		if err != nil {
			yield(nil, errors.New(ErrTableListFailed, "failed to list tables", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var tableNamespace, tableName string
			if err := rows.Scan(&tableNamespace, &tableName); err != nil {
				yield(nil, errors.New(ErrTableScanFailed, "failed to scan table row", err))
				return
			}

			ns := stringToNamespace(tableNamespace)
			identifier := append(ns, tableName)
			if !yield(identifier, nil) {
				return
			}
		}

		if err := rows.Err(); err != nil {
			yield(nil, errors.New(ErrTableIterationFailed, "error iterating table rows", err))
		}
	}
}

// CreateNamespace creates a new namespace
func (c *Catalog) CreateNamespace(ctx context.Context, namespace table.Identifier, props iceberg.Properties) error {
	// Check if namespace already exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return errors.New(ErrNamespaceCheckFailed, "failed to check namespace existence", err)
	}
	if exists {
		return catalog.ErrNamespaceAlreadyExists
	}

	namespaceStr := namespaceToString(namespace)

	// Insert default property to mark namespace as existing
	insertSQL := `INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, 'exists', 'true')`
	_, err = c.db.ExecContext(ctx, insertSQL, c.name, namespaceStr)
	if err != nil {
		return errors.New(ErrNamespaceCreateFailed, "failed to create namespace", err)
	}

	// Insert additional properties
	for key, value := range props {
		insertPropSQL := `INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, ?, ?)`
		_, err = c.db.ExecContext(ctx, insertPropSQL, c.name, namespaceStr, key, value)
		if err != nil {
			return errors.New(ErrNamespaceInsertFailed, "failed to insert namespace property", err)
		}
	}

	return nil
}

// DropNamespace drops a namespace from the catalog
func (c *Catalog) DropNamespace(ctx context.Context, namespace table.Identifier) error {
	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return errors.New(ErrNamespaceCheckFailed, "failed to check namespace existence", err)
	}
	if !exists {
		return catalog.ErrNoSuchNamespace
	}

	namespaceStr := namespaceToString(namespace)

	// Check if namespace has tables
	var tableCount int
	countQuery := `SELECT COUNT(*) FROM iceberg_tables WHERE catalog_name = ? AND table_namespace = ?`
	err = c.db.QueryRowContext(ctx, countQuery, c.name, namespaceStr).Scan(&tableCount)
	if err != nil {
		return errors.New(ErrNamespaceCountFailed, "failed to count tables in namespace", err)
	}
	if tableCount > 0 {
		return catalog.ErrNamespaceNotEmpty
	}

	// Delete namespace properties
	deleteSQL := `DELETE FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ?`
	_, err = c.db.ExecContext(ctx, deleteSQL, c.name, namespaceStr)
	if err != nil {
		return errors.New(ErrNamespaceDeleteFailed, "failed to delete namespace", err)
	}

	return nil
}

// CheckNamespaceExists checks if a namespace exists
func (c *Catalog) CheckNamespaceExists(ctx context.Context, namespace table.Identifier) (bool, error) {
	namespaceStr := namespaceToString(namespace)

	var count int
	query := `SELECT COUNT(*) FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = 'exists'`
	err := c.db.QueryRowContext(ctx, query, c.name, namespaceStr).Scan(&count)
	if err != nil {
		return false, errors.New(ErrNamespaceCheckFailed, "failed to check namespace existence", err)
	}

	return count > 0, nil
}

// LoadNamespaceProperties loads properties for a namespace
func (c *Catalog) LoadNamespaceProperties(ctx context.Context, namespace table.Identifier) (iceberg.Properties, error) {
	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return nil, errors.New(ErrNamespaceCheckFailed, "failed to check namespace existence", err)
	}
	if !exists {
		return nil, catalog.ErrNoSuchNamespace
	}

	namespaceStr := namespaceToString(namespace)
	props := make(iceberg.Properties)

	query := `SELECT property_key, property_value FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ?`
	rows, err := c.db.QueryContext(ctx, query, c.name, namespaceStr)
	if err != nil {
		return nil, errors.New(ErrPropertiesLoadFailed, "failed to load namespace properties", err)
	}
	defer rows.Close()

	for rows.Next() {
		var key string
		var value sql.NullString
		if err := rows.Scan(&key, &value); err != nil {
			return nil, errors.New(ErrPropertiesScanFailed, "failed to scan property row", err)
		}

		if value.Valid {
			props[key] = value.String
		}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.New(ErrPropertiesIterationFailed, "error iterating property rows", err)
	}

	return props, nil
}

// UpdateNamespaceProperties updates properties for a namespace
func (c *Catalog) UpdateNamespaceProperties(ctx context.Context, namespace table.Identifier, removals []string, updates iceberg.Properties) (catalog.PropertiesUpdateSummary, error) {
	// Check if namespace exists
	exists, err := c.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, errors.New(ErrNamespaceCheckFailed, "failed to check namespace existence", err)
	}
	if !exists {
		return catalog.PropertiesUpdateSummary{}, catalog.ErrNoSuchNamespace
	}

	namespaceStr := namespaceToString(namespace)
	var removed, updated, missing []string

	// Begin transaction for atomic updates
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return catalog.PropertiesUpdateSummary{}, errors.New(ErrPropertiesTransactionFailed, "failed to begin transaction", err)
	}
	defer func() {
		if err := tx.Rollback(); err != nil {
			// Log error but don't override original error
			log.Printf("Failed to rollback transaction: %v", err)
		}
	}()

	// Handle removals
	for _, key := range removals {
		if key == "exists" {
			// Don't allow removing the 'exists' property
			missing = append(missing, key)
			continue
		}

		deleteSQL := `DELETE FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`
		result, err := tx.ExecContext(ctx, deleteSQL, c.name, namespaceStr, key)
		if err != nil {
			return catalog.PropertiesUpdateSummary{}, errors.New(ErrPropertiesRemoveFailed, fmt.Sprintf("failed to remove property %s", key), err)
		}

		rowsAffected, _ := result.RowsAffected()
		if rowsAffected > 0 {
			removed = append(removed, key)
		} else {
			missing = append(missing, key)
		}
	}

	// Handle updates/additions
	for key, value := range updates {
		// Check if property exists
		var count int
		checkSQL := `SELECT COUNT(*) FROM iceberg_namespace_properties WHERE catalog_name = ? AND namespace = ? AND property_key = ?`
		err := tx.QueryRowContext(ctx, checkSQL, c.name, namespaceStr, key).Scan(&count)
		if err != nil {
			return catalog.PropertiesUpdateSummary{}, errors.New(ErrPropertiesCheckFailed, "failed to check property existence", err)
		}

		if count > 0 {
			// Update existing property
			updateSQL := `UPDATE iceberg_namespace_properties SET property_value = ? WHERE catalog_name = ? AND namespace = ? AND property_key = ?`
			_, err = tx.ExecContext(ctx, updateSQL, value, c.name, namespaceStr, key)
		} else {
			// Insert new property
			insertSQL := `INSERT INTO iceberg_namespace_properties (catalog_name, namespace, property_key, property_value) VALUES (?, ?, ?, ?)`
			_, err = tx.ExecContext(ctx, insertSQL, c.name, namespaceStr, key, value)
		}

		if err != nil {
			return catalog.PropertiesUpdateSummary{}, errors.New(ErrPropertiesUpdateFailed, fmt.Sprintf("failed to update property %s", key), err)
		}

		updated = append(updated, key)
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return catalog.PropertiesUpdateSummary{}, errors.New(ErrPropertiesCommitFailed, "failed to commit transaction", err)
	}

	return catalog.PropertiesUpdateSummary{
		Removed: removed,
		Updated: updated,
		Missing: missing,
	}, nil
}

// ListNamespaces lists all namespaces, optionally filtered by parent
func (c *Catalog) ListNamespaces(ctx context.Context, parent table.Identifier) ([]table.Identifier, error) {
	query := `SELECT DISTINCT namespace FROM iceberg_namespace_properties WHERE catalog_name = ?`
	rows, err := c.db.QueryContext(ctx, query, c.name)
	if err != nil {
		return nil, errors.New(ErrNamespaceListFailed, "failed to list namespaces", err)
	}
	defer rows.Close()

	var namespaces []table.Identifier
	for rows.Next() {
		var namespaceStr string
		if err := rows.Scan(&namespaceStr); err != nil {
			return nil, errors.New(ErrNamespaceScanFailed, "failed to scan namespace row", err)
		}

		namespace := stringToNamespace(namespaceStr)

		// Filter by parent if specified
		if len(parent) > 0 {
			if len(namespace) <= len(parent) {
				continue
			}
			match := true
			for i, part := range parent {
				if i >= len(namespace) || namespace[i] != part {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		namespaces = append(namespaces, namespace)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.New(ErrNamespaceIterationFailed, "error iterating namespace rows", err)
	}

	return namespaces, nil
}

// Helper functions

func namespaceToString(namespace table.Identifier) string {
	if len(namespace) == 0 {
		return ""
	}

	result := ""
	for i, part := range namespace {
		if i > 0 {
			result += "."
		}
		result += part
	}
	return result
}

func stringToNamespace(namespaceStr string) table.Identifier {
	if namespaceStr == "" {
		return table.Identifier{}
	}

	return catalog.ToIdentifier(namespaceStr)
}

func (c *Catalog) defaultTableLocation(identifier table.Identifier) string {
	// Use PathManager for table location
	namespace := identifier[:len(identifier)-1]
	tableName := identifier[len(identifier)-1]
	return c.pathManager.GetTableDataPath(namespace, tableName)
}

func (c *Catalog) newMetadataLocation(identifier table.Identifier, version int) string {
	// Use PathManager for metadata location
	namespace := identifier[:len(identifier)-1]
	tableName := identifier[len(identifier)-1]

	metadataDir := c.pathManager.GetTableMetadataPath(namespace, tableName)
	filename := fmt.Sprintf("v%d.metadata.json", version)
	return filepath.Join(metadataDir, filename)
}

// getNextMetadataVersion determines the next version number for metadata files
func (c *Catalog) getNextMetadataVersion(currentMetadataLocation string) int {
	if currentMetadataLocation == "" {
		return 1
	}

	// Extract version from current metadata location
	// Expected format: .../metadata/v{version}.metadata.json
	filename := filepath.Base(currentMetadataLocation)
	if strings.HasPrefix(filename, "v") && strings.HasSuffix(filename, ".metadata.json") {
		versionStr := filename[1:strings.Index(filename, ".")]
		if version, err := strconv.Atoi(versionStr); err == nil {
			return version + 1
		}
	}

	// Default to version 2 if we can't parse the current version
	return 2
}

// writeMetadataFile writes the metadata to the specified location
func (c *Catalog) writeMetadataFile(metadata table.Metadata, metadataLocation string) error {
	// Serialize metadata to JSON
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return errors.New(ErrMetadataSerializeFailed, "failed to serialize metadata", err)
	}

	// Use local file operations for catalog metadata
	// Note: This method is for catalog metadata files, not warehouse data
	return writeFile(metadataLocation, metadataJSON)
}

// Helper methods for metadata operations

// writeFile writes data to a file (helper function)
func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}

	file, err := os.Create(path)
	if err != nil {
		return errors.New(ErrMetadataFileCreateFailed, fmt.Sprintf("failed to create file %s", path), err)
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return errors.New(ErrMetadataFileWriteFailed, fmt.Sprintf("failed to write to file %s", path), err)
	}

	return nil
}
