package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// Config represents the client configuration
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Auth     AuthConfig     `yaml:"auth"`
	Database DatabaseConfig `yaml:"database"`
	SSL      SSLConfig      `yaml:"ssl"`
	Logging  LogConfig      `yaml:"logging"`
}

// ServerConfig holds server connection configuration
type ServerConfig struct {
	Address string        `yaml:"address"`
	Port    int           `yaml:"port"`
	Timeout time.Duration `yaml:"timeout"`
}

// AuthConfig holds authentication configuration
type AuthConfig struct {
	Username string `yaml:"username"`
	Password string `yaml:"password"`
}

// DatabaseConfig holds database configuration
type DatabaseConfig struct {
	Name string `yaml:"name"`
}

// SSLConfig holds SSL configuration
type SSLConfig struct {
	Mode     string `yaml:"mode"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// LogConfig holds logging configuration
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// DefaultConfig returns default client configuration
func DefaultConfig() *Config {
	return &Config{
		Server: ServerConfig{
			Address: "localhost",
			Port:    8080,
			Timeout: 30 * time.Second,
		},
		Auth: AuthConfig{
			Username: "",
			Password: "",
		},
		Database: DatabaseConfig{
			Name: "default",
		},
		SSL: SSLConfig{
			Mode: "disable",
		},
		Logging: LogConfig{
			Level:  "info",
			Format: "text",
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
	if _, err := os.Stat("icebox-client.yml"); err == nil {
		return "icebox-client.yml"
	}

	// Check home directory
	homeDir, err := os.UserHomeDir()
	if err == nil {
		configPath := filepath.Join(homeDir, ".icebox", "icebox-client.yml")
		if _, err := os.Stat(configPath); err == nil {
			return configPath
		}
	}

	// Check /etc/icebox
	if _, err := os.Stat("/etc/icebox/icebox-client.yml"); err == nil {
		return "/etc/icebox/icebox-client.yml"
	}

	return ""
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if c.Server.Address == "" {
		return fmt.Errorf("server address cannot be empty")
	}

	if c.Server.Port <= 0 || c.Server.Port > 65535 {
		return fmt.Errorf("invalid server port: %d", c.Server.Port)
	}

	return nil
}

// GetServerURL returns the full server URL
func (c *Config) GetServerURL() string {
	protocol := "http"
	if c.SSL.Mode != "disable" {
		protocol = "https"
	}
	return fmt.Sprintf("%s://%s:%d", protocol, c.Server.Address, c.Server.Port)
}
