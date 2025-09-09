package registry

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/gear6io/ranger/pkg/errors"
)

// RetrieveAllSchemas retrieves all schema data using Bun relational queries
func (sm *Store) RetrieveAllSchemas(ctx context.Context) (map[string]*SchemaData, error) {
	var databases []DatabaseWithRelations

	// Bun handles all the JOINs automatically based on relations
	err := sm.bunMigrator.GetDB().NewSelect().
		Model(&databases).
		Relation("Tables.Columns").
		Where("databases.deleted_at IS NULL").
		Where("tables.deleted_at IS NULL").
		Scan(ctx)

	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to retrieve all schemas", err)
	}

	return sm.buildSchemaDataFromRelations(databases), nil
}

// RetrieveSchema loads schema for a specific table and database using Bun relational queries
func (sm *Store) RetrieveSchema(ctx context.Context, database, tableName string) (*SchemaData, error) {
	var table TableWithRelations

	err := sm.bunMigrator.GetDB().NewSelect().
		Model(&table).
		Relation("Database").
		Relation("Columns").
		Where("databases.name = ?", database).
		Where("tables.name = ?", tableName).
		Where("databases.deleted_at IS NULL").
		Where("tables.deleted_at IS NULL").
		Scan(ctx)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, errors.New(RegistryTableNotFound, "table not found", nil).
				AddContext("database", database).
				AddContext("table", tableName)
		}
		return nil, errors.New(errors.CommonInternal, "failed to retrieve schema", err)
	}

	return sm.buildSchemaDataFromTable(table), nil
}

// SchemaLoaderFunc is a function type for loading schemas
type SchemaLoaderFunc func(ctx context.Context, database, tableName string) (*SchemaData, error)

// CreateSchemaDataLoader creates a schema data loader function that returns raw registry data
func (sm *Store) CreateSchemaDataLoader() SchemaLoaderFunc {
	return func(ctx context.Context, database, tableName string) (*SchemaData, error) {
		return sm.RetrieveSchema(ctx, database, tableName)
	}
}

// buildSchemaDataFromRelations converts database relations to SchemaData map
func (sm *Store) buildSchemaDataFromRelations(databases []DatabaseWithRelations) map[string]*SchemaData {
	schemas := make(map[string]*SchemaData)

	for _, db := range databases {
		for _, table := range db.Tables {
			cacheKey := fmt.Sprintf("%s.%s", db.Name, table.Name)
			schemas[cacheKey] = &SchemaData{
				Database: db.Name,
				Table:    table.Name,
				TableID:  table.ID,
				Columns:  table.Columns, // Bun already loaded these
				// Note: Metadata fields are now part of the Table struct itself
			}
		}
	}

	return schemas
}

// buildSchemaDataFromTable converts table relation to SchemaData
func (sm *Store) buildSchemaDataFromTable(table TableWithRelations) *SchemaData {
	return &SchemaData{
		Database: table.Database.Name,
		Table:    table.Name,
		TableID:  table.ID,
		Columns:  table.Columns, // Bun already loaded these
		// Note: Metadata fields are now part of the Table struct itself
	}
}
