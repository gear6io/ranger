package main

import (
	"fmt"
	"io/ioutil"

	"gopkg.in/yaml.v3"
)

// Config represents the ErrorCode checker configuration
type Config struct {
	ExcludePaths               []string `yaml:"exclude_paths"`
	ForbiddenPatterns          []string `yaml:"forbidden_patterns"`
	CheckForbidden             bool     `yaml:"check_forbidden"`
	CheckInternal              bool     `yaml:"check_internal"`
	CheckInternalPackageCalls  bool     `yaml:"check_internal_package_calls"`
	CheckEnhancedPatterns      bool     `yaml:"check_enhanced_patterns"`
	OutputFormat               string   `yaml:"output_format"`
	ExitOnUnused               bool     `yaml:"exit_on_unused"`
	ExitOnForbidden            bool     `yaml:"exit_on_forbidden"`
	ExitOnInternal             bool     `yaml:"exit_on_internal"`
	ExitOnInternalPackageCalls bool     `yaml:"exit_on_internal_package_calls"`
	ExitOnEnhancedPatterns     bool     `yaml:"exit_on_enhanced_patterns"`
	Verbose                    bool     `yaml:"verbose"`
}

// loadConfig loads configuration from file or uses defaults
func loadConfig(configPath string) (*Config, error) {
	config := &Config{
		ExcludePaths:               []string{"pkg/sdk/", "integration_tests/", "testdata/", "milestones/", "scripts/", "demo/", "data/", "logs/", "workspace/", "my-lakehouse/", "vendor/", "node_modules/", ".git/"},
		ForbiddenPatterns:          []string{`fmt\.Errorf`, `errors\.New\(`, `errors\.Wrap\(`, `errors\.Wrapf\(`},
		CheckForbidden:             true,
		CheckInternal:              true,
		CheckInternalPackageCalls:  true,
		CheckEnhancedPatterns:      true,
		OutputFormat:               "human",
		ExitOnUnused:               false,
		ExitOnForbidden:            false,
		ExitOnInternal:             false,
		ExitOnInternalPackageCalls: false,
		ExitOnEnhancedPatterns:     false, // Default to false - these are suggestions, not errors
		Verbose:                    false,
	}

	if configPath != "" {
		data, err := ioutil.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	return config, nil
}
