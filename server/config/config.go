package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config represents the server configuration
type Config struct {
	HTTP   HTTPConfig   `yaml:"http"`
	JDBC   JDBCConfig   `yaml:"jdbc"`
	Native NativeConfig `yaml:"native"`
	Log    LogConfig    `yaml:"log"`
}

// HTTPConfig represents HTTP server configuration
type HTTPConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

// JDBCConfig represents JDBC server configuration
type JDBCConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
}

// NativeConfig represents native protocol server configuration
type NativeConfig struct {
	Enabled bool   `yaml:"enabled"`
	Address string `yaml:"address"`
	Port    int    `yaml:"port"`
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

// LoadDefaultConfig returns a default configuration
func LoadDefaultConfig() *Config {
	return &Config{
		HTTP: HTTPConfig{
			Enabled: true,
			Address: "0.0.0.0",
			Port:    8080,
		},
		JDBC: JDBCConfig{
			Enabled: true,
			Address: "0.0.0.0",
			Port:    5432,
		},
		Native: NativeConfig{
			Enabled: true,
			Address: "0.0.0.0",
			Port:    9000,
		},
		Log: LogConfig{
			Level:      "info",
			Format:     "console",
			FilePath:   "logs/icebox-server.log",
			Console:    true,
			MaxSize:    100, // 100MB
			MaxBackups: 3,
			MaxAge:     7, // 7 days
			Cleanup:    true, // Cleanup log file on startup by default
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
	if c.HTTP.Enabled {
		if c.HTTP.Port <= 0 || c.HTTP.Port > 65535 {
			return fmt.Errorf("invalid HTTP port: %d", c.HTTP.Port)
		}
	}

	if c.JDBC.Enabled {
		if c.JDBC.Port <= 0 || c.JDBC.Port > 65535 {
			return fmt.Errorf("invalid JDBC port: %d", c.JDBC.Port)
		}
	}

	if c.Native.Enabled {
		if c.Native.Port <= 0 || c.Native.Port > 65535 {
			return fmt.Errorf("invalid native port: %d", c.Native.Port)
		}
	}

	return nil
}
