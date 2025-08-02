package client

import (
	"context"
	"fmt"
	"time"

	"github.com/TFMV/icebox/client/config"
	"github.com/TFMV/icebox/client/protocols/http"
	"github.com/rs/zerolog"
)

// Client represents the main icebox client
type Client struct {
	config     *config.Config
	httpClient *http.Client
	logger     zerolog.Logger
	connected  bool
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string
	Rows     [][]interface{}
	RowCount int64
	Duration time.Duration
}

// New creates a new icebox client
func New(cfg *config.Config, logger zerolog.Logger) (*Client, error) {
	// Create HTTP client
	httpClient, err := http.NewClient(&cfg.Server, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP client: %w", err)
	}

	return &Client{
		config:     cfg,
		httpClient: httpClient,
		logger:     logger,
		connected:  false,
	}, nil
}

// Connect establishes a connection to the server
func (c *Client) Connect(ctx context.Context) error {
	c.logger.Debug().Msg("Connecting to icebox server")

	// Test connection
	if err := c.httpClient.Ping(ctx); err != nil {
		return fmt.Errorf("failed to connect to server: %w", err)
	}

	c.connected = true
	c.logger.Info().Msg("Connected to icebox server")
	return nil
}

// Close closes the client connection
func (c *Client) Close() error {
	c.logger.Debug().Msg("Closing client connection")
	c.connected = false
	return nil
}

// ExecuteQuery executes a SQL query
func (c *Client) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return nil, err
		}
	}

	c.logger.Debug().Str("query", query).Msg("Executing query")

	// Execute query via HTTP client
	result, err := c.httpClient.ExecuteQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	return &QueryResult{
		Columns:  result.Columns,
		Rows:     result.Rows,
		RowCount: result.RowCount,
		Duration: result.Duration,
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

	// Import file via HTTP client
	return c.httpClient.ImportFile(ctx, filePath, tableName, namespace, overwrite)
}

// ListTables lists all tables
func (c *Client) ListTables(ctx context.Context) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Msg("Listing tables")

	// List tables via HTTP client
	tables, err := c.httpClient.ListTables(ctx)
	if err != nil {
		return fmt.Errorf("failed to list tables: %w", err)
	}

	// Display tables
	fmt.Printf("📋 Tables:\n")
	for _, table := range tables {
		fmt.Printf("   - %s\n", table)
	}

	return nil
}

// DescribeTable describes a table structure
func (c *Client) DescribeTable(ctx context.Context, tableName string) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Str("table", tableName).Msg("Describing table")

	// Describe table via HTTP client
	schema, err := c.httpClient.DescribeTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("failed to describe table: %w", err)
	}

	// Display schema
	fmt.Printf("📋 Table: %s\n", tableName)
	fmt.Printf("📊 Schema:\n")
	for _, column := range schema.Columns {
		fmt.Printf("   - %s: %s\n", column.Name, column.Type)
	}

	return nil
}

// DropTable drops a table
func (c *Client) DropTable(ctx context.Context, tableName string) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Str("table", tableName).Msg("Dropping table")

	// Drop table via HTTP client
	return c.httpClient.DropTable(ctx, tableName)
}

// ListNamespaces lists all namespaces
func (c *Client) ListNamespaces(ctx context.Context) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Msg("Listing namespaces")

	// List namespaces via HTTP client
	namespaces, err := c.httpClient.ListNamespaces(ctx)
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	// Display namespaces
	fmt.Printf("📋 Namespaces:\n")
	for _, namespace := range namespaces {
		fmt.Printf("   - %s\n", namespace)
	}

	return nil
}

// CreateNamespace creates a new namespace
func (c *Client) CreateNamespace(ctx context.Context, namespace string) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Str("namespace", namespace).Msg("Creating namespace")

	// Create namespace via HTTP client
	return c.httpClient.CreateNamespace(ctx, namespace)
}

// DropNamespace drops a namespace
func (c *Client) DropNamespace(ctx context.Context, namespace string) error {
	if !c.connected {
		if err := c.Connect(ctx); err != nil {
			return err
		}
	}

	c.logger.Debug().Str("namespace", namespace).Msg("Dropping namespace")

	// Drop namespace via HTTP client
	return c.httpClient.DropNamespace(ctx, namespace)
}

// IsConnected returns whether the client is connected
func (c *Client) IsConnected() bool {
	return c.connected
}

// GetConfig returns the client configuration
func (c *Client) GetConfig() *config.Config {
	return c.config
}
