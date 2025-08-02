package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the main Icebox configuration
type Config struct {
	Name     string        `yaml:"name"`
	Version  string        `yaml:"version,omitempty"`
	Catalog  CatalogConfig `yaml:"catalog"`
	Storage  StorageConfig `yaml:"storage"`
	Metadata Metadata      `yaml:"metadata,omitempty"`
}

// CatalogConfig holds catalog-specific configuration
type CatalogConfig struct {
	Type   string        `yaml:"type"`
	SQLite *SQLiteConfig `yaml:"sqlite,omitempty"`
	REST   *RESTConfig   `yaml:"rest,omitempty"`
	JSON   *JSONConfig   `yaml:"json,omitempty"`
}

// SQLiteConfig holds SQLite catalog configuration
type SQLiteConfig struct {
	Path string `yaml:"path"`
}

// RESTConfig holds REST catalog configuration
type RESTConfig struct {
	URI               string            `yaml:"uri"`
	Credentials       map[string]string `yaml:"credentials,omitempty"`
	OAuth             *OAuthConfig      `yaml:"oauth,omitempty"`
	SigV4             *SigV4Config      `yaml:"sigv4,omitempty"`
	TLS               *TLSConfig        `yaml:"tls,omitempty"`
	WarehouseLocation string            `yaml:"warehouse_location,omitempty"`
	MetadataLocation  string            `yaml:"metadata_location,omitempty"`
	Prefix            string            `yaml:"prefix,omitempty"`
	AdditionalProps   map[string]string `yaml:"additional_properties,omitempty"`
}

// OAuthConfig holds OAuth authentication configuration
type OAuthConfig struct {
	Token      string `yaml:"token,omitempty"`
	Credential string `yaml:"credential,omitempty"`
	AuthURL    string `yaml:"auth_url,omitempty"`
	Scope      string `yaml:"scope,omitempty"`
}

// SigV4Config holds AWS Signature Version 4 authentication configuration
type SigV4Config struct {
	Enabled bool   `yaml:"enabled"`
	Region  string `yaml:"region,omitempty"`
	Service string `yaml:"service,omitempty"`
}

// TLSConfig holds TLS configuration
type TLSConfig struct {
	SkipVerify bool `yaml:"skip_verify,omitempty"`
}

// StorageConfig holds storage-specific configuration
type StorageConfig struct {
	Type       string            `yaml:"type"`
	FileSystem *FileSystemConfig `yaml:"filesystem,omitempty"`
	Memory     *MemoryConfig     `yaml:"memory,omitempty"`
	S3         *S3Config         `yaml:"s3,omitempty"`
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

// Metadata holds additional project metadata
type Metadata struct {
	CreatedAt   string            `yaml:"created_at,omitempty"`
	Description string            `yaml:"description,omitempty"`
	Tags        []string          `yaml:"tags,omitempty"`
	Properties  map[string]string `yaml:"properties,omitempty"`
}

// JSONConfig holds JSON catalog configuration
type JSONConfig struct {
	URI       string `yaml:"uri"`       // Path to the catalog.json file
	Warehouse string `yaml:"warehouse"` // Warehouse root path for table storage
}

// WriteConfig writes a configuration to a YAML file
func WriteConfig(path string, cfg *Config) error {
	// Set default version if not specified
	if cfg.Version == "" {
		cfg.Version = "1"
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create config file: %w", err)
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	defer encoder.Close()

	if err := encoder.Encode(cfg); err != nil {
		return fmt.Errorf("failed to encode config: %w", err)
	}

	return nil
}

// ReadConfig reads a configuration from a YAML file
func ReadConfig(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var cfg Config
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	return &cfg, nil
}

// FindConfig searches for a .icebox.yml file in the current directory or parents
func FindConfig() (string, *Config, error) {
	// Start from current directory and walk up
	currentDir, err := os.Getwd()
	if err != nil {
		return "", nil, fmt.Errorf("failed to get current directory: %w", err)
	}

	configPath, err := findConfigFile(currentDir)
	if err != nil {
		return "", nil, err
	}

	cfg, err := ReadConfig(configPath)
	if err != nil {
		return "", nil, err
	}

	return configPath, cfg, nil
}

// findConfigFile searches for .icebox.yml starting from the given directory
func findConfigFile(startDir string) (string, error) {
	currentDir := startDir

	for {
		configPath := fmt.Sprintf("%s/.icebox.yml", currentDir)
		if _, err := os.Stat(configPath); err == nil {
			return configPath, nil
		}

		// Move up one directory
		parentDir := fmt.Sprintf("%s/..", currentDir)
		absParent, err := os.Getwd()
		if err != nil {
			return "", fmt.Errorf("failed to resolve parent directory: %w", err)
		}

		// Check if we've reached the root
		if absParent == currentDir {
			break
		}

		currentDir = parentDir
	}

	return "", fmt.Errorf("no .icebox.yml found in current directory or parents")
}
