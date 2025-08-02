package display

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// Config represents the display configuration
type Config struct {
	Theme       string            `yaml:"theme" json:"theme"`
	Format      string            `yaml:"format" json:"format"`
	Table       TableConfig       `yaml:"table" json:"table"`
	Colors      ColorConfig       `yaml:"colors" json:"colors"`
	Interactive InteractiveConfig `yaml:"interactive" json:"interactive"`
	Verbose     bool              `yaml:"verbose" json:"verbose"`
	Timing      bool              `yaml:"timing" json:"timing"`
}

// TableConfig represents table-specific configuration
type TableConfig struct {
	MaxWidth       int  `yaml:"max_width" json:"max_width"`
	Pagination     int  `yaml:"pagination" json:"pagination"`
	UnicodeBorders bool `yaml:"unicode_borders" json:"unicode_borders"`
	ShowRowNumbers bool `yaml:"show_row_numbers" json:"show_row_numbers"`
	CompactMode    bool `yaml:"compact_mode" json:"compact_mode"`
}

// ColorConfig represents color configuration
type ColorConfig struct {
	Enabled string `yaml:"enabled" json:"enabled"` // auto, always, never
}

// InteractiveConfig represents interactive feature configuration
type InteractiveConfig struct {
	ConfirmDestructive bool `yaml:"confirm_destructive" json:"confirm_destructive"`
	ShowHints          bool `yaml:"show_hints" json:"show_hints"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	return &Config{
		Theme:  "default",
		Format: "table",
		Table: TableConfig{
			MaxWidth:       120,
			Pagination:     50,
			UnicodeBorders: true,
			ShowRowNumbers: false,
			CompactMode:    false,
		},
		Colors: ColorConfig{
			Enabled: "auto",
		},
		Interactive: InteractiveConfig{
			ConfirmDestructive: true,
			ShowHints:          true,
		},
		Verbose: false,
		Timing:  false,
	}
}

// LoadConfig loads configuration from the default location
func LoadConfig() (*Config, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return DefaultConfig(), nil
	}

	configPath := filepath.Join(homeDir, ".icebox", "display.yaml")
	return LoadConfigFromFile(configPath)
}

// LoadConfigFromFile loads configuration from a specific file
func LoadConfigFromFile(path string) (*Config, error) {
	config := DefaultConfig()

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return config, nil
		}
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	// Try YAML first
	if err := yaml.Unmarshal(data, config); err != nil {
		// Try JSON as fallback
		if err := json.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	return config, nil
}

// SaveConfig saves configuration to the default location
func SaveConfig(config *Config) error {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return fmt.Errorf("failed to get home directory: %w", err)
	}

	configDir := filepath.Join(homeDir, ".icebox")
	if err := os.MkdirAll(configDir, 0755); err != nil {
		return fmt.Errorf("failed to create config directory: %w", err)
	}

	configPath := filepath.Join(configDir, "display.yaml")
	return SaveConfigToFile(config, configPath)
}

// SaveConfigToFile saves configuration to a specific file
func SaveConfigToFile(config *Config, path string) error {
	data, err := yaml.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// ApplyConfig applies configuration to a display instance
func (d *DisplayImpl) ApplyConfig(config *Config) {
	// Apply theme
	switch config.Theme {
	case "dark":
		d.theme = DarkTheme
	case "light":
		d.theme = LightTheme
	case "minimal":
		d.theme = MinimalTheme
	default:
		d.theme = DefaultTheme
	}

	// Apply format
	switch config.Format {
	case "csv":
		d.format = FormatCSV
	case "json":
		d.format = FormatJSON
	case "markdown":
		d.format = FormatMarkdown
	default:
		d.format = FormatTable
	}

	// Apply color settings
	if config.Colors.Enabled == "never" ||
		(config.Colors.Enabled == "auto" && !d.caps.SupportsColor) {
		// Switch to minimal theme if colors are disabled
		d.theme = MinimalTheme
	}
}

// NewWithConfig creates a new display instance with configuration
func NewWithConfig(config *Config) *DisplayImpl {
	d := New()
	d.ApplyConfig(config)
	return d
}
