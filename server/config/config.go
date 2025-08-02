package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the server configuration
type Config struct {
	Version string        `yaml:"version"`
	HTTP    HTTPConfig    `yaml:"http"`
	JDBC    JDBCConfig    `yaml:"jdbc"`
	Storage StorageConfig `yaml:"storage"`
	Logging LogConfig     `yaml:"logging"`
}

// HTTPConfig holds HTTP server configuration
type HTTPConfig struct {
	Enabled      bool          `yaml:"enabled"`
	Address      string        `yaml:"address"`
	Port         int           `yaml:"port"`
	ReadTimeout  time.Duration `yaml:"read_timeout"`
	WriteTimeout time.Duration `yaml:"write_timeout"`
	IdleTimeout  time.Duration `yaml:"idle_timeout"`
	CORS         CORSConfig    `yaml:"cors"`
	TLS          TLSConfig     `yaml:"tls"`
}

// JDBCConfig holds JDBC server configuration
type JDBCConfig struct {
	Enabled           bool          `yaml:"enabled"`
	Address           string        `yaml:"address"`
	Port              int           `yaml:"port"`
	MaxConnections    int           `yaml:"max_connections"`
	ConnectionTimeout time.Duration `yaml:"connection_timeout"`
	QueryTimeout      time.Duration `yaml:"query_timeout"`
	TLS               TLSConfig     `yaml:"tls"`
}

// StorageConfig holds storage configuration
type StorageConfig struct {
	Type       string           `yaml:"type"`
	FileSystem FileSystemConfig `yaml:"filesystem,omitempty"`
	Memory     MemoryConfig     `yaml:"memory,omitempty"`
	S3         S3Config         `yaml:"s3,omitempty"`
	Catalog    CatalogConfig    `yaml:"catalog"`
}

// FileSystemConfig holds local filesystem storage configuration
type FileSystemConfig struct {
	RootPath string `yaml:"root_path"`
}

// MemoryConfig holds in-memory storage configuration
type MemoryConfig struct {
	// No specific configuration needed for memory storage
}

// S3Config holds S3-compatible storage configuration
type S3Config struct {
	Bucket          string `yaml:"bucket"`
	Region          string `yaml:"region,omitempty"`
	Endpoint        string `yaml:"endpoint,omitempty"`
	AccessKeyID     string `yaml:"access_key_id,omitempty"`
	SecretAccessKey string `yaml:"secret_access_key,omitempty"`
}

// CatalogConfig holds catalog configuration
type CatalogConfig struct {
	Type   string       `yaml:"type"`
	SQLite SQLiteConfig `yaml:"sqlite,omitempty"`
	REST   RESTConfig   `yaml:"rest,omitempty"`
	JSON   JSONConfig   `yaml:"json,omitempty"`
}

// SQLiteConfig holds SQLite catalog configuration
type SQLiteConfig struct {
	Path string `yaml:"path"`
}

// RESTConfig holds REST catalog configuration
type RESTConfig struct {
	URI               string            `yaml:"uri"`
	Credentials       map[string]string `yaml:"credentials,omitempty"`
	WarehouseLocation string            `yaml:"warehouse_location,omitempty"`
}

// JSONConfig holds JSON catalog configuration
type JSONConfig struct {
	URI       string `yaml:"uri"`
	Warehouse string `yaml:"warehouse"`
}

// CORSConfig holds CORS configuration
type CORSConfig struct {
	Enabled      bool     `yaml:"enabled"`
	AllowOrigins []string `yaml:"allow_origins"`
	AllowMethods []string `yaml:"allow_methods"`
	AllowHeaders []string `yaml:"allow_headers"`
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	File   string `yaml:"file,omitempty"`
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Version: "0.1.0",
		HTTP: HTTPConfig{
			Enabled:      true,
			Address:      "0.0.0.0",
			Port:         8080,
			ReadTimeout:  30 * time.Second,
			WriteTimeout: 30 * time.Second,
			IdleTimeout:  60 * time.Second,
			CORS: CORSConfig{
				Enabled:      true,
				AllowOrigins: []string{"*"},
				AllowMethods: []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
				AllowHeaders: []string{"Origin", "Content-Type", "Accept", "Authorization"},
			},
		},
		JDBC: JDBCConfig{
			Enabled:           true,
			Address:           "0.0.0.0",
			Port:              5432,
			MaxConnections:    50,
			ConnectionTimeout: 30 * time.Second,
			QueryTimeout:      300 * time.Second,
		},
		Storage: StorageConfig{
			Type: "filesystem",
			FileSystem: FileSystemConfig{
				RootPath: "./data",
			},
			Catalog: CatalogConfig{
				Type: "sqlite",
				SQLite: SQLiteConfig{
					Path: "./catalog.db",
				},
			},
		},
		Logging: LogConfig{
			Level:  "info",
			Format: "json",
		},
	}
}

// Load loads configuration from file or environment
func Load() (*Config, error) {
	// Try to find config file
	configPath := findConfigFile()

	if configPath != "" {
		return LoadFromFile(configPath)
	}

	// Return default config if no file found
	return DefaultConfig(), nil
}

// LoadFromFile loads configuration from a specific file
func LoadFromFile(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	cfg := DefaultConfig()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}

	return cfg, nil
}

// Save saves configuration to file
func (c *Config) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// findConfigFile searches for configuration file
func findConfigFile() string {
	// Check current directory
	if _, err := os.Stat("icebox-server.yml"); err == nil {
		return "icebox-server.yml"
	}

	// Check home directory
	homeDir, err := os.UserHomeDir()
	if err == nil {
		configPath := filepath.Join(homeDir, ".icebox", "icebox-server.yml")
		if _, err := os.Stat(configPath); err == nil {
			return configPath
		}
	}

	// Check /etc/icebox
	if _, err := os.Stat("/etc/icebox/icebox-server.yml"); err == nil {
		return "/etc/icebox/icebox-server.yml"
	}

	return ""
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.HTTP.Enabled {
		if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
			return fmt.Errorf("invalid HTTP port: %d", c.HTTP.Port)
		}
	}

	if c.JDBC.Enabled {
		if c.JDBC.Port <= 0 || c.JDBC.Port > 65535 {
			return fmt.Errorf("invalid JDBC port: %d", c.JDBC.Port)
		}
		if c.JDBC.MaxConnections <= 0 {
			return fmt.Errorf("invalid max connections: %d", c.JDBC.MaxConnections)
		}
	}

	return nil
}
