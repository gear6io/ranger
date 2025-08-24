package client

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/icebox/client/config"
	"github.com/TFMV/icebox/pkg/errors"
	"github.com/TFMV/icebox/pkg/sdk"
	"github.com/rs/zerolog"
)

// Client represents the main icebox client
type Client struct {
	config    *config.Config
	sdkClient *sdk.Client
	logger    zerolog.Logger
	connected bool
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	RowCount int64
	Duration time.Duration
}

// TableSchema represents a table schema
type TableSchema struct {
	Columns []Column `json:"columns"`
}

// Column represents a table column
type Column struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// New creates a new icebox client
func New(cfg *config.Config, logger zerolog.Logger) (*Client, error) {
	// Create SDK client options
	options := &sdk.Options{
		Protocol: sdk.Native,
		Addr:     []string{fmt.Sprintf("%s:%d", cfg.Server.Address, cfg.Server.Port)},
		Auth: sdk.Auth{
			Username: cfg.Auth.Username,
			Password: cfg.Auth.Password,
			Database: cfg.Database.Name,
		},
		DialTimeout:  cfg.Server.Timeout,
		ReadTimeout:  cfg.Server.Timeout,
		WriteTimeout: cfg.Server.Timeout,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		Settings: sdk.Settings{
			"use_native": true,
		},
	}

	// Create SDK client
	sdkClient, err := sdk.NewClient(options)
	if err != nil {
		return nil, errors.New(ErrSDKClientCreationFailed, "failed to create SDK client", err)
	}

	return &Client{
		config:    cfg,
		sdkClient: sdkClient,
		logger:    logger,
		connected: false,
	}, nil
}

// Connect establishes a connection to the server
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Debug().Msg("Connecting to icebox server")

	// Test connection using ping
	if err := c.sdkClient.Ping(ctx); err != nil {
		return errors.New(ErrConnectionFailed, "failed to connect to server", err)
	}

	c.connected = true
	c.logger.Info().Msg("Connected to icebox server")
	return nil
}

// Close closes the client connection
func (c *Client) Close() error {
	c.logger.Debug().Msg("Closing client connection")
	c.connected = false
	return c.sdkClient.Close()
}

// ExecuteQuery executes a SQL query
func (c *Client) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	// Don't try to connect if already connected
	if !c.connected {
		return nil, errors.New(ErrClientNotConnected, "client not connected to server", nil)
	}

	c.logger.Debug().Str("query", query).Msg("Executing query")

	start := time.Now()

	// Execute query using SDK
	rows, err := c.sdkClient.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Extract columns
	var columns []string
	for _, col := range rows.Cols {
		columns = append(columns, col.Name)
	}

	// Extract data
	var data [][]interface{}
	for rows.Next() {
		// Get the current row data from the rows
		// rows.Current is 1-indexed and gets advanced by Next()
		if rows.Current > 0 && rows.Current <= len(rows.Data) {
			rowData := rows.Data[rows.Current-1]
			if rowData != nil {
				// Create a copy of the row data to avoid reference issues
				rowCopy := make([]interface{}, len(rowData))
				copy(rowCopy, rowData)
				data = append(data, rowCopy)
			}
		}
	}

	// Check for any errors that occurred during row iteration
	if err := rows.Err(); err != nil {
		return nil, err
	}

	duration := time.Since(start)

	return &QueryResult{
		Columns:  columns,
		Rows:     data,
		RowCount: int64(len(data)),
		Duration: duration,
	}, nil
}

// ImportFile imports data from a file
func (c *Client) ImportFile(ctx context.Context, filePath, tableName, namespace string, overwrite bool) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().
		Str("file", filePath).
		Str("table", tableName).
		Str("namespace", namespace).
		Bool("overwrite", overwrite).
		Msg("Importing file")

	// Use SQL COPY command for import
	var query string
	if overwrite {
		query = fmt.Sprintf("COPY %s FROM '%s' (FORMAT PARQUET)", tableName, filePath)
	} else {
		query = fmt.Sprintf("COPY %s FROM '%s' (FORMAT PARQUET)", tableName, filePath)
	}

	// Execute the import query
	if err := c.sdkClient.Exec(ctx, query); err != nil {
		return errors.New(ErrFileImportFailed, "failed to import file", err)
	}

	return nil
}

// ListTables lists all tables
func (c *Client) ListTables(ctx context.Context) ([]string, error) {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	c.logger.Debug().Msg("Listing tables")

	// Execute SHOW TABLES query
	result, err := c.ExecuteQuery(ctx, "SHOW TABLES")
	if err != nil {
		return nil, errors.New(ErrTableListFailed, "failed to list tables", err)
	}

	// Extract table names from the result
	var tables []string
	for _, row := range result.Rows {
		if len(row) > 0 {
			if tableName, ok := row[0].(string); ok {
				tables = append(tables, tableName)
			}
		}
	}

	return tables, nil
}

// DescribeTable describes a table structure
func (c *Client) DescribeTable(ctx context.Context, tableName string) (*TableSchema, error) {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	c.logger.Debug().Str("table", tableName).Msg("Describing table")

	// Execute DESCRIBE query
	result, err := c.ExecuteQuery(ctx, fmt.Sprintf("DESCRIBE %s", tableName))
	if err != nil {
		return nil, errors.New(ErrTableDescribeFailed, "failed to describe table", err)
	}

	// Extract schema information from the result
	schema := &TableSchema{
		Columns: []Column{},
	}

	for _, row := range result.Rows {
		if len(row) >= 2 {
			columnName, _ := row[0].(string)
			columnType, _ := row[1].(string)

			if columnName != "" && columnType != "" {
				schema.Columns = append(schema.Columns, Column{
					Name: columnName,
					Type: columnType,
				})
			}
		}
	}

	return schema, nil
}

// DropTable drops a table
func (c *Client) DropTable(ctx context.Context, tableName string) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Str("table", tableName).Msg("Dropping table")

	// Execute DROP TABLE query
	if err := c.sdkClient.Exec(ctx, fmt.Sprintf("DROP TABLE %s", tableName)); err != nil {
		return errors.New(ErrTableDropFailed, "failed to drop table", err)
	}

	return nil
}

// ListNamespaces lists all namespaces
func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	c.logger.Debug().Msg("Listing namespaces")

	// Try to get namespaces using SQL query
	// If it fails, return default namespace
	result, err := c.ExecuteQuery(ctx, "SELECT DISTINCT namespace FROM system.namespaces")
	if err != nil {
		// If the query fails, return default namespace
		return []string{"default"}, nil
	}

	// Extract namespace names from the result
	var namespaces []string
	for _, row := range result.Rows {
		if len(row) > 0 {
			if namespace, ok := row[0].(string); ok {
				namespaces = append(namespaces, namespace)
			}
		}
	}

	// If no namespaces found, return default
	if len(namespaces) == 0 {
		namespaces = []string{"default"}
	}

	return namespaces, nil
}

// CreateNamespace creates a new namespace
func (c *Client) CreateNamespace(ctx context.Context, namespace string) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Str("namespace", namespace).Msg("Creating namespace")

	// Execute CREATE NAMESPACE query
	if err := c.sdkClient.Exec(ctx, fmt.Sprintf("CREATE NAMESPACE %s", namespace)); err != nil {
		return errors.New(ErrNamespaceCreationFailed, "failed to create namespace", err)
	}

	return nil
}

// DropNamespace drops a namespace
func (c *Client) DropNamespace(ctx context.Context, namespace string) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Str("namespace", namespace).Msg("Dropping namespace")

	// Execute DROP NAMESPACE query
	if err := c.sdkClient.Exec(ctx, fmt.Sprintf("DROP NAMESPACE %s", namespace)); err != nil {
		return errors.New(ErrNamespaceDropFailed, "failed to drop namespace", err)
	}

	return nil
}

// IsConnected returns whether the client is connected
func (c *Client) IsConnected() bool {
	return c.connected
}

// GetConfig returns the client configuration
func (c *Client) GetConfig() *config.Config {
	return c.config
}
