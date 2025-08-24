package config

import (
	"fmt"
	"os"

	"github.com/TFMV/icebox/pkg/errors"
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
	DataPath string        `yaml:"data_path"`
	Catalog  CatalogConfig `yaml:"catalog"`
	Data     DataConfig    `yaml:"data"`
}

// CatalogConfig represents catalog configuration
type CatalogConfig struct {
	Type string `yaml:"type"`
}

// DataConfig represents data storage configuration
// Note: Storage engine is now specified per-table, not globally
type DataConfig struct {
	// Engine-specific configurations can be added here later
	// For now, all engines are available at runtime
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
			DataPath: "./data", // Default data path
			Catalog: CatalogConfig{
				Type: "json",
			},
			Data: DataConfig{
				// All storage engines are now available at runtime
			},
		},
	}
}

// LoadConfig loads configuration from a file
func LoadConfig(filename string) (*Config, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, errors.New(ErrConfigFileReadFailed, "failed to read config file", err)
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, errors.New(ErrConfigFileParseFailed, "failed to parse config file", err)
	}

	// Validate the loaded configuration
	if err := config.Validate(); err != nil {
		return nil, errors.New(ErrConfigValidationFailed, "configuration validation failed", err)
	}

	// Log the data path being used
	fmt.Printf("📁 Using data path: %s\n", config.GetStoragePath())
	fmt.Printf("🗄️  Catalog type: %s\n", config.GetCatalogType())
	fmt.Printf("💾 Storage engines: All available at runtime (per-table selection)\n")

	return &config, nil
}

// SaveConfig saves configuration to a file
func SaveConfig(config *Config, filename string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return errors.New(ErrConfigFileMarshalFailed, "failed to marshal config", err)
	}

	if err := os.WriteFile(filename, data, 0644); err != nil {
		return errors.New(ErrConfigFileWriteFailed, "failed to write config file", err)
	}

	return nil
}

// Validate validates the configuration
func (c *Config) Validate() error {
	// Validate storage configuration
	if err := c.Storage.Validate(); err != nil {
		return errors.New(ErrStorageValidationFailed, "storage validation failed", err)
	}

	// Port validation is no longer needed since ports are fixed
	// Address validation could be added here if needed
	return nil
}

// Validate validates the storage configuration
func (s *StorageConfig) Validate() error {
	// Validate catalog configuration
	if err := s.Catalog.Validate(); err != nil {
		return errors.New(ErrCatalogValidationFailed, "catalog validation failed", err)
	}

	// Validate data storage configuration
	if err := s.Data.Validate(); err != nil {
		return errors.New(ErrDataStorageValidationFailed, "data storage validation failed", err)
	}

	// Validate data_path
	if s.DataPath == "" {
		return errors.New(ErrDataPathRequired, "data_path is required in storage configuration", nil)
	}

	return nil
}

// Validate validates the catalog configuration
func (c *CatalogConfig) Validate() error {
	if c.Type == "" {
		return errors.New(ErrCatalogTypeRequired, "catalog type is required", nil)
	}

	return nil
}

// Validate validates the data storage configuration
func (d *DataConfig) Validate() error {
	// Storage type is now specified per-table, not globally
	// All engines are available at runtime
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

// GetStorageType returns the storage type
// Note: This method is deprecated. Storage engine is now specified per-table.
func (c *Config) GetStorageType() string {
	// Return default for backward compatibility
	// In the new multi-engine architecture, storage type is specified per-table
	return "filesystem"
}

// GetStoragePath returns the storage path
func (c *Config) GetStoragePath() string {
	return c.Storage.DataPath
}

// GetCatalogType returns the catalog type
func (c *Config) GetCatalogType() string {
	return c.Storage.Catalog.Type
}
