package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/TFMV/icebox/client/config"
	"github.com/rs/zerolog"
)

// Client represents an HTTP client for icebox server
type Client struct {
	config  *config.ServerConfig
	client  *http.Client
	logger  zerolog.Logger
	baseURL string
}

// QueryResult represents the result of a SQL query
type QueryResult struct {
	Columns  []string        `json:"columns"`
	Rows     [][]interface{} `json:"rows"`
	RowCount int64           `json:"row_count"`
	Duration time.Duration   `json:"duration"`
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

// NewClient creates a new HTTP client
func NewClient(cfg *config.ServerConfig, logger zerolog.Logger) (*Client, error) {
	client := &http.Client{
		Timeout: cfg.Timeout,
	}

	baseURL := fmt.Sprintf("http://%s:%d", cfg.Address, cfg.Port)

	return &Client{
		config:  cfg,
		client:  client,
		logger:  logger,
		baseURL: baseURL,
	}, nil
}

// Ping tests the connection to the server
func (c *Client) Ping(ctx context.Context) error {
	url := fmt.Sprintf("%s/health", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to ping server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned status: %d", resp.StatusCode)
	}

	return nil
}

// ExecuteQuery executes a SQL query
func (c *Client) ExecuteQuery(ctx context.Context, query string) (*QueryResult, error) {
	url := fmt.Sprintf("%s/api/v1/query/sql", c.baseURL)

	requestBody := map[string]string{
		"query": query,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var result QueryResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &result, nil
}

// ImportFile imports data from a file
func (c *Client) ImportFile(ctx context.Context, filePath, tableName, namespace string, overwrite bool) error {
	url := fmt.Sprintf("%s/api/v1/import/parquet", c.baseURL)

	requestBody := map[string]interface{}{
		"file_path":  filePath,
		"table_name": tableName,
		"namespace":  namespace,
		"overwrite":  overwrite,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to import file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ListTables lists all tables
func (c *Client) ListTables(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/query/tables", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list tables: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Tables []string `json:"tables"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return response.Tables, nil
}

// DescribeTable describes a table structure
func (c *Client) DescribeTable(ctx context.Context, tableName string) (*TableSchema, error) {
	url := fmt.Sprintf("%s/api/v1/namespaces/default/tables/%s/schema", c.baseURL, tableName)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to describe table: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var schema TableSchema
	if err := json.NewDecoder(resp.Body).Decode(&schema); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return &schema, nil
}

// DropTable drops a table
func (c *Client) DropTable(ctx context.Context, tableName string) error {
	url := fmt.Sprintf("%s/api/v1/namespaces/default/tables/%s", c.baseURL, tableName)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to drop table: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// ListNamespaces lists all namespaces
func (c *Client) ListNamespaces(ctx context.Context) ([]string, error) {
	url := fmt.Sprintf("%s/api/v1/catalog/namespaces", c.baseURL)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	var response struct {
		Namespaces [][]string `json:"namespaces"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// Extract namespace names from the response
	var namespaces []string
	for _, ns := range response.Namespaces {
		if len(ns) > 0 {
			namespaces = append(namespaces, ns[0])
		}
	}

	return namespaces, nil
}

// CreateNamespace creates a new namespace
func (c *Client) CreateNamespace(ctx context.Context, namespace string) error {
	url := fmt.Sprintf("%s/api/v1/catalog/namespaces", c.baseURL)

	requestBody := map[string]string{
		"namespace": namespace,
	}

	jsonBody, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to create namespace: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// DropNamespace drops a namespace
func (c *Client) DropNamespace(ctx context.Context, namespace string) error {
	url := fmt.Sprintf("%s/api/v1/catalog/namespaces/%s", c.baseURL, namespace)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to drop namespace: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server returned status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
