package registry

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/rs/zerolog"
)

// Package-specific error codes for CDC operations
var (
	RegistryCDCCreationFailed = errors.MustNewCode("registry.cdc_creation_failed")
	RegistryCDCIndexFailed    = errors.MustNewCode("registry.cdc_index_failed")
	RegistryCDCTriggerFailed  = errors.MustNewCode("registry.cdc_trigger_failed")
)

// CDCLogEntry represents a raw change from the CDC log table
type CDCLogEntry struct {
	ID        int64  `json:"id"`
	Timestamp string `json:"timestamp"`
	TableName string `json:"tablename"`
	Operation string `json:"operation"`
	Before    string `json:"before"` // JSON string of OLD values
	After     string `json:"after"`  // JSON string of NEW values
	CreatedAt string `json:"created_at"`
}

// CDCSetup manages the CDC table and triggers
type CDCSetup struct {
	db              *sql.DB
	logger          zerolog.Logger
	logTable        string
	monitoredTables []string
}

// NewCDCSetup creates a new CDC setup manager
func NewCDCSetup(db *sql.DB, logger zerolog.Logger) *CDCSetup {
	return &CDCSetup{
		db:       db,
		logger:   logger,
		logTable: "__cdc_log",
		monitoredTables: []string{
			"tables",
			"table_files",
			"table_statistics",
			"table_metadata",
		},
	}
}

// SetupCDC creates the CDC log table and triggers for monitored tables
func (c *CDCSetup) SetupCDC(ctx context.Context) error {
	c.logger.Info().Msg("Setting up CDC infrastructure")

	// Create CDC log table
	if err := c.createCDCLogTable(ctx); err != nil {
		return errors.New(RegistryCDCCreationFailed, "failed to create CDC log table", err)
	}

	// Create triggers for each monitored table
	for _, table := range c.monitoredTables {
		if err := c.createTableTriggers(ctx, table); err != nil {
			return errors.New(RegistryCDCTriggerFailed, "failed to create triggers for table", err).AddContext("table", table)
		}
	}

	c.logger.Info().Msg("CDC infrastructure setup completed")
	return nil
}

// TeardownCDC removes CDC triggers and table
func (c *CDCSetup) TeardownCDC(ctx context.Context) error {
	c.logger.Info().Msg("Tearing down CDC infrastructure")

	// Drop triggers for each monitored table
	for _, table := range c.monitoredTables {
		if err := c.dropTableTriggers(ctx, table); err != nil {
			c.logger.Warn().Err(err).Str("table", table).Msg("Failed to drop triggers for table")
		}
	}

	// Drop CDC log table
	if err := c.dropCDCLogTable(ctx); err != nil {
		return errors.New(RegistryCDCCreationFailed, "failed to drop CDC log table", err)
	}

	c.logger.Info().Msg("CDC infrastructure teardown completed")
	return nil
}

// createCDCLogTable creates the CDC log table
func (c *CDCSetup) createCDCLogTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			timestamp TEXT NOT NULL,
			tablename TEXT NOT NULL,
			operation TEXT NOT NULL,
			before TEXT,
			after TEXT,
			created_at TEXT NOT NULL
		)
	`, c.logTable)

	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return errors.New(RegistryCDCCreationFailed, "failed to create CDC log table", err)
	}

	// Create indexes for performance
	indexes := []string{
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_timestamp ON %s(timestamp)", c.logTable, c.logTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_tablename ON %s(tablename)", c.logTable, c.logTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_operation ON %s(operation)", c.logTable, c.logTable),
		fmt.Sprintf("CREATE INDEX IF NOT EXISTS idx_%s_created ON %s(created_at)", c.logTable, c.logTable),
	}

	for _, index := range indexes {
		if _, err := c.db.ExecContext(ctx, index); err != nil {
			return errors.New(RegistryCDCIndexFailed, "failed to create CDC index", err)
		}
	}

	c.logger.Debug().Str("table", c.logTable).Msg("Created CDC log table with indexes")
	return nil
}

// dropCDCLogTable drops the CDC log table
func (c *CDCSetup) dropCDCLogTable(ctx context.Context) error {
	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", c.logTable)
	_, err := c.db.ExecContext(ctx, query)
	if err != nil {
		return errors.New(RegistryCDCCreationFailed, "failed to drop CDC log table", err)
	}

	c.logger.Debug().Str("table", c.logTable).Msg("Dropped CDC log table")
	return nil
}

// createTableTriggers creates CDC triggers for a specific table
func (c *CDCSetup) createTableTriggers(ctx context.Context, tableName string) error {
	// Get table columns for JSON object generation
	columns, err := c.getTableColumns(ctx, tableName)
	if err != nil {
		return errors.New(RegistryCDCTriggerFailed, "failed to get columns for table", err).AddContext("table", tableName)
	}

	// Create INSERT trigger
	insertTrigger := c.buildInsertTrigger(tableName, columns)
	if _, err := c.db.ExecContext(ctx, insertTrigger); err != nil {
		return errors.New(RegistryCDCTriggerFailed, "failed to create INSERT trigger for table", err).AddContext("table", tableName)
	}

	// Create UPDATE trigger
	updateTrigger := c.buildUpdateTrigger(tableName, columns)
	if _, err := c.db.ExecContext(ctx, updateTrigger); err != nil {
		return errors.New(RegistryCDCTriggerFailed, "failed to create UPDATE trigger for table", err).AddContext("table", tableName)
	}

	// Create DELETE trigger
	deleteTrigger := c.buildDeleteTrigger(tableName, columns)
	if _, err := c.db.ExecContext(ctx, deleteTrigger); err != nil {
		return errors.New(RegistryCDCTriggerFailed, "failed to create DELETE trigger for table", err).AddContext("table", tableName)
	}

	// Create updated_at trigger
	updatedAtTrigger := c.buildUpdatedAtTrigger(tableName)
	if _, err := c.db.ExecContext(ctx, updatedAtTrigger); err != nil {
		return errors.New(RegistryCDCTriggerFailed, "failed to create updated_at trigger for table", err).AddContext("table", tableName)
	}

	c.logger.Debug().Str("table", tableName).Msg("Created CDC triggers for table")
	return nil
}

// dropTableTriggers drops CDC triggers for a specific table
func (c *CDCSetup) dropTableTriggers(ctx context.Context, tableName string) error {
	triggers := []string{
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s__cdc_insert", tableName),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s__cdc_update", tableName),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s__cdc_delete", tableName),
		fmt.Sprintf("DROP TRIGGER IF EXISTS %s__updated_at", tableName),
	}

	for _, trigger := range triggers {
		if _, err := c.db.ExecContext(ctx, trigger); err != nil {
			c.logger.Warn().Err(err).Str("trigger", trigger).Msg("Failed to drop trigger")
		}
	}

	c.logger.Debug().Str("table", tableName).Msg("Dropped CDC triggers for table")
	return nil
}

// getTableColumns retrieves column information for a table
func (c *CDCSetup) getTableColumns(ctx context.Context, tableName string) ([]string, error) {
	query := fmt.Sprintf("PRAGMA table_info(%s)", tableName)
	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var columns []string
	for rows.Next() {
		var (
			cid       int
			name      string
			typ       string
			notnull   int
			dfltValue interface{}
			pk        int
		)
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dfltValue, &pk); err != nil {
			return nil, err
		}
		columns = append(columns, name)
	}

	return columns, nil
}

// buildInsertTrigger builds the INSERT trigger SQL
func (c *CDCSetup) buildInsertTrigger(tableName string, columns []string) string {
	jsonObject := c.buildJSONObject("NEW.", columns)
	return fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS %s__cdc_insert 
		AFTER INSERT ON %s 
		BEGIN
			INSERT INTO %s (timestamp, tablename, operation, before, after, created_at) 
			VALUES (datetime('now', 'subsec'), '%s', 'INSERT', NULL, %s, datetime('now'));
		END
	`, tableName, tableName, c.logTable, tableName, jsonObject)
}

// buildUpdateTrigger builds the UPDATE trigger SQL
func (c *CDCSetup) buildUpdateTrigger(tableName string, columns []string) string {
	oldJSON := c.buildJSONObject("OLD.", columns)
	newJSON := c.buildJSONObject("NEW.", columns)
	return fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS %s__cdc_update 
		AFTER UPDATE ON %s 
		BEGIN
			INSERT INTO %s (timestamp, tablename, operation, before, after, created_at) 
			VALUES (datetime('now', 'subsec'), '%s', 'UPDATE', %s, %s, datetime('now'));
		END
	`, tableName, tableName, c.logTable, tableName, oldJSON, newJSON)
}

// buildDeleteTrigger builds the DELETE trigger SQL
func (c *CDCSetup) buildDeleteTrigger(tableName string, columns []string) string {
	jsonObject := c.buildJSONObject("OLD.", columns)
	return fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS %s__cdc_delete 
		AFTER DELETE ON %s 
		BEGIN
			INSERT INTO %s (timestamp, tablename, operation, before, after, created_at) 
			VALUES (datetime('now', 'subsec'), '%s', 'DELETE', %s, NULL, datetime('now'));
		END
	`, tableName, tableName, c.logTable, tableName, jsonObject)
}

// buildUpdatedAtTrigger builds the updated_at trigger SQL
func (c *CDCSetup) buildUpdatedAtTrigger(tableName string) string {
	return fmt.Sprintf(`
		CREATE TRIGGER IF NOT EXISTS %s__updated_at 
		AFTER UPDATE ON %s 
		BEGIN
			UPDATE %s SET updated_at = datetime('now') WHERE id = NEW.id;
		END
	`, tableName, tableName, tableName)
}

// buildJSONObject builds a JSON object from column names
func (c *CDCSetup) buildJSONObject(prefix string, columns []string) string {
	var pairs []string
	for _, col := range columns {
		pairs = append(pairs, fmt.Sprintf("'%s', %s%s", col, prefix, col))
	}
	return fmt.Sprintf("json_object(%s)", strings.Join(pairs, ", "))
}

// GetCDCLogTable returns the CDC log table name
func (c *CDCSetup) GetCDCLogTable() string {
	return c.logTable
}

// GetMonitoredTables returns the list of monitored tables
func (c *CDCSetup) GetMonitoredTables() []string {
	return c.monitoredTables
}
