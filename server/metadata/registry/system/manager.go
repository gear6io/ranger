package system

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gear6io/ranger/pkg/errors"
)

// Manager manages the system database views and queries
type Manager struct {
	db *sql.DB
}

// NewManager creates a new system database manager
func NewManager(db *sql.DB) *Manager {
	return &Manager{
		db: db,
	}
}

// Initialize creates or recreates all system views from embedded SQL files
func (m *Manager) Initialize(ctx context.Context) error {
	// Get all SQL files from embedded FS
	entries, err := fs.ReadDir(ViewsFS, "views")
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to read views directory", err)
	}

	// Sort entries by name for deterministic execution order
	var sqlFiles []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			sqlFiles = append(sqlFiles, entry.Name())
		}
	}
	sort.Strings(sqlFiles)

	// Execute each SQL file in a transaction
	tx, err := m.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.New(errors.CommonInternal, "failed to begin transaction", err)
	}
	defer tx.Rollback()

	for _, sqlFile := range sqlFiles {
		// Read SQL content
		content, err := fs.ReadFile(ViewsFS, filepath.Join("views", sqlFile))
		if err != nil {
			return errors.New(errors.CommonInternal, "failed to read SQL file", err).AddContext("file", sqlFile)
		}

		// Execute SQL
		_, err = tx.ExecContext(ctx, string(content))
		if err != nil {
			return errors.New(errors.CommonInternal, "failed to execute SQL", err).
				AddContext("file", sqlFile).
				AddContext("sql", string(content))
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return errors.New(errors.CommonInternal, "failed to commit transaction", err)
	}

	return nil
}

// Query executes a query against the system database
func (m *Manager) Query(ctx context.Context, query string) (*QueryResult, error) {
	// Execute the query
	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to execute system database query", err).
			AddContext("query", query)
	}
	defer rows.Close()

	// Get column information
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to get column information", err)
	}

	// Read all rows
	var data [][]interface{}
	for rows.Next() {
		// Create a slice of interface{} to hold the values
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		// Scan the row
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan row", err)
		}

		data = append(data, values)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.New(errors.CommonInternal, "error iterating rows", err)
	}

	return &QueryResult{
		Columns:  columns,
		Data:     data,
		RowCount: int64(len(data)),
	}, nil
}

// GetSystemDatabases returns all databases from the system view
func (m *Manager) GetSystemDatabases(ctx context.Context) ([]*SystemDatabase, error) {
	query := `SELECT database_name, display_name, description, is_system, is_read_only, table_count, total_size, created_at, updated_at FROM system.databases`

	rows, err := m.db.QueryContext(ctx, query)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to query system databases", err)
	}
	defer rows.Close()

	var databases []*SystemDatabase
	for rows.Next() {
		var db SystemDatabase
		err := rows.Scan(
			&db.Name,
			&db.DisplayName,
			&db.Description,
			&db.IsSystem,
			&db.IsReadOnly,
			&db.TableCount,
			&db.TotalSize,
			&db.CreatedAt,
			&db.UpdatedAt,
		)
		if err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan system database", err)
		}
		databases = append(databases, &db)
	}

	return databases, nil
}

// GetSystemTables returns all tables from the system view
func (m *Manager) GetSystemTables(ctx context.Context, databaseName string) ([]*SystemTable, error) {
	query := `SELECT database_name, table_name, display_name, description, table_type, is_temporary, is_external, row_count, file_count, total_size, created_at, updated_at FROM system.tables WHERE database_name = ?`

	rows, err := m.db.QueryContext(ctx, query, databaseName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to query system tables", err)
	}
	defer rows.Close()

	var tables []*SystemTable
	for rows.Next() {
		var table SystemTable
		err := rows.Scan(
			&table.DatabaseName,
			&table.TableName,
			&table.DisplayName,
			&table.Description,
			&table.TableType,
			&table.IsTemporary,
			&table.IsExternal,
			&table.RowCount,
			&table.FileCount,
			&table.TotalSize,
			&table.CreatedAt,
			&table.UpdatedAt,
		)
		if err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan system table", err)
		}
		tables = append(tables, &table)
	}

	return tables, nil
}

// GetSystemColumns returns all columns from the system view
func (m *Manager) GetSystemColumns(ctx context.Context, databaseName, tableName string) ([]*SystemColumn, error) {
	query := `SELECT database_name, table_name, column_name, display_name, data_type, is_nullable, is_primary, is_unique, default_value, description, ordinal_position, max_length, precision, scale, created_at, updated_at FROM system.columns WHERE database_name = ? AND table_name = ? ORDER BY ordinal_position`

	rows, err := m.db.QueryContext(ctx, query, databaseName, tableName)
	if err != nil {
		return nil, errors.New(errors.CommonInternal, "failed to query system columns", err)
	}
	defer rows.Close()

	var columns []*SystemColumn
	for rows.Next() {
		var column SystemColumn
		err := rows.Scan(
			&column.DatabaseName,
			&column.TableName,
			&column.ColumnName,
			&column.DisplayName,
			&column.DataType,
			&column.IsNullable,
			&column.IsPrimary,
			&column.IsUnique,
			&column.DefaultValue,
			&column.Description,
			&column.OrdinalPosition,
			&column.MaxLength,
			&column.Precision,
			&column.Scale,
			&column.CreatedAt,
			&column.UpdatedAt,
		)
		if err != nil {
			return nil, errors.New(errors.CommonInternal, "failed to scan system column", err)
		}
		columns = append(columns, &column)
	}

	return columns, nil
}

// GenerateCreateTableDDL generates CREATE TABLE DDL from metadata
func (m *Manager) GenerateCreateTableDDL(ctx context.Context, databaseName, tableName string) (string, error) {
	// Get table metadata first
	tableQuery := `SELECT table_type, is_temporary, is_external FROM system.tables WHERE database_name = ? AND table_name = ?`
	var tableType string
	var isTemporary, isExternal bool
	err := m.db.QueryRowContext(ctx, tableQuery, databaseName, tableName).Scan(&tableType, &isTemporary, &isExternal)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", errors.New(errors.CommonNotFound, "table not found", nil).
				AddContext("database", databaseName).
				AddContext("table", tableName)
		}
		return "", errors.New(errors.CommonInternal, "failed to get table metadata", err)
	}

	// Get columns
	columns, err := m.GetSystemColumns(ctx, databaseName, tableName)
	if err != nil {
		return "", errors.New(errors.CommonInternal, "failed to get table columns", err)
	}

	// Generate DDL
	var ddl strings.Builder

	// Table type prefix
	if isTemporary {
		ddl.WriteString("CREATE TEMPORARY TABLE ")
	} else if isExternal {
		ddl.WriteString("CREATE EXTERNAL TABLE ")
	} else {
		ddl.WriteString("CREATE TABLE ")
	}

	ddl.WriteString(fmt.Sprintf("%s.%s (\n", databaseName, tableName))

	var columnDefs []string
	for _, col := range columns {
		colDef := fmt.Sprintf("  %s %s", col.ColumnName, col.DataType)

		if !col.IsNullable {
			colDef += " NOT NULL"
		}

		if col.IsPrimary {
			colDef += " PRIMARY KEY"
		}

		if col.IsUnique {
			colDef += " UNIQUE"
		}

		if col.DefaultValue != "" {
			colDef += fmt.Sprintf(" DEFAULT %s", col.DefaultValue)
		}

		columnDefs = append(columnDefs, colDef)
	}

	ddl.WriteString(strings.Join(columnDefs, ",\n"))
	ddl.WriteString("\n)")

	// Add table properties
	if tableType != "user" {
		ddl.WriteString(fmt.Sprintf(" WITH (type = '%s')", tableType))
	}

	ddl.WriteString(";")

	return ddl.String(), nil
}

// IsSystemDatabaseQuery checks if a query targets the system database
func (m *Manager) IsSystemDatabaseQuery(query string) bool {
	query = strings.ToUpper(strings.TrimSpace(query))
	return strings.Contains(query, "SYSTEM.") ||
		strings.Contains(query, "FROM SYSTEM") ||
		strings.Contains(query, "JOIN SYSTEM")
}

// QueryResult represents the result of a system database query
type QueryResult struct {
	Columns  []string
	Data     [][]interface{}
	RowCount int64
}

// SystemDatabase represents a database from the system view
type SystemDatabase struct {
	Name        string
	DisplayName string
	Description string
	IsSystem    bool
	IsReadOnly  bool
	TableCount  int
	TotalSize   int64
	CreatedAt   string
	UpdatedAt   string
}

// SystemTable represents a table from the system view
type SystemTable struct {
	DatabaseName string
	TableName    string
	DisplayName  string
	Description  string
	TableType    string
	IsTemporary  bool
	IsExternal   bool
	RowCount     int64
	FileCount    int
	TotalSize    int64
	CreatedAt    string
	UpdatedAt    string
}

// SystemColumn represents a column from the system view
type SystemColumn struct {
	DatabaseName    string
	TableName       string
	ColumnName      string
	DisplayName     string
	DataType        string
	IsNullable      bool
	IsPrimary       bool
	IsUnique        bool
	DefaultValue    string
	Description     string
	OrdinalPosition int
	MaxLength       int
	Precision       int
	Scale           int
	CreatedAt       string
	UpdatedAt       string
}

