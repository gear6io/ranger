package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the server configuration
type Config struct {
	Log     LogConfig     `yaml:"log"`
	Storage StorageConfig `yaml:"storage"`
}

// LogConfig represents logging configuration
type LogConfig struct {
	Level      string `yaml:"level"`
	Format     string `yaml:"format"`      // "json" or "console"
	FilePath   string `yaml:"file_path"`   // Path to log file
	Console    bool   `yaml:"console"`     // Whether to log to console
	MaxSize    int    `yaml:"max_size"`    // Max file size in MB
	MaxBackups int    `yaml:"max_backups"` // Max number of backup files
	MaxAge     int    `yaml:"max_age"`     // Max age in days
	Cleanup    bool   `yaml:"cleanup"`     // Whether to cleanup log file on startup
}

// StorageConfig represents storage configuration
type StorageConfig struct {
	Catalog CatalogConfig `yaml:"catalog"`
	Path    string        `yaml:"path"`
}

// CatalogConfig represents catalog configuration
type CatalogConfig struct {
	Type string `yaml:"type"`
	URI  string `yaml:"uri"`
}

// LoadDefaultConfig returns a default configuration
func LoadDefaultConfig() *Config {
	return &Config{
		Log: LogConfig{
			Level:      "info",
			Format:     "console",
			FilePath:   "logs/icebox-server.log",
			Console:    true,
			MaxSize:    100, // 100MB
			MaxBackups: 3,
			MaxAge:     7,    // 7 days
			Cleanup:    true, // Cleanup log file on startup by default
		},
		Storage: StorageConfig{
			Catalog: CatalogConfig{
				Type: "file",
				URI:  "file:///var/lib/icebox/catalog",
			},
			Path: "data",
		},
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return &config, nil
}

// SaveConfig saves configuration to a file
func SaveConfig(config *Config, filename string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Port validation is no longer needed since ports are fixed
	// Address validation could be added here if needed
	return nil
}

// GetHTTPPort returns the fixed HTTP server port
func (c *Config) GetHTTPPort() int {
	return HTTP_SERVER_PORT
}

// GetJDBCPort returns the fixed JDBC server port
func (c *Config) GetJDBCPort() int {
	return JDBC_SERVER_PORT
}

// GetNativePort returns the fixed native protocol server port
func (c *Config) GetNativePort() int {
	return NATIVE_SERVER_PORT
}

// GetMinIOPort returns the fixed MinIO server port
func (c *Config) GetMinIOPort() int {
	return MINIO_SERVER_PORT
}

// GetHealthCheckPort returns the health check port
func (c *Config) GetHealthCheckPort() int {
	return HEALTH_CHECK_PORT
}

// GetHTTPAddress returns the HTTP server address
func (c *Config) GetHTTPAddress() string {
	return DEFAULT_SERVER_ADDRESS
}

// GetJDBCAddress returns the JDBC server address
func (c *Config) GetJDBCAddress() string {
	return DEFAULT_SERVER_ADDRESS
}

// GetNativeAddress returns the native server address
func (c *Config) GetNativeAddress() string {
	return DEFAULT_SERVER_ADDRESS
}

// IsHTTPServerEnabled returns whether the HTTP server is enabled
func (c *Config) IsHTTPServerEnabled() bool {
	return HTTP_SERVER_ENABLED
}

// IsJDBCServerEnabled returns whether the JDBC server is enabled
func (c *Config) IsJDBCServerEnabled() bool {
	return JDBC_SERVER_ENABLED
}

// IsNativeServerEnabled returns whether the native server is enabled
func (c *Config) IsNativeServerEnabled() bool {
	return NATIVE_SERVER_ENABLED
}

// GetStoragePath returns the storage path
func (c *Config) GetStoragePath() string {
	return c.Storage.Path
}

// GetCatalogType returns the catalog type
func (c *Config) GetCatalogType() string {
	return c.Storage.Catalog.Type
}

// GetCatalogURI returns the catalog URI
func (c *Config) GetCatalogURI() string {
	return c.Storage.Catalog.URI
}
