package registry

import (
	"github.com/gear6io/ranger/server/metadata/registry/regtypes"
)

// TableWithRelations represents a table with all its related data loaded
type TableWithRelations struct {
	regtypes.Table `bun:",inherit"`

	// Relations - Bun handles the JOINs automatically
	Database *regtypes.Database      `bun:"rel:belongs-to,join:database_id=id"`
	Columns  []*regtypes.TableColumn `bun:"rel:has-many,join:id=table_id"`
	// Note: TableMetadata fields are now part of the Table struct itself
}

// DatabaseWithRelations represents a database with all its tables and related data loaded
type DatabaseWithRelations struct {
	regtypes.Database `bun:",inherit"`

	// Relations
	Tables []*TableWithRelations `bun:"rel:has-many,join:id=database_id"`
}
