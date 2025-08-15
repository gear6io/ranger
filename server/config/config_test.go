package config

import (
	"os"
	"strings"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Create a temporary config file
	configContent := `# Icebox Server Configuration
version: "0.1.0"

# Logging Configuration
log:
  level: "debug"
  format: "console"
  file_path: "logs/icebox-server.log"
  console: true
  max_size: 100
  max_backups: 3
  max_age: 7
  cleanup: true

# Storage Configuration
storage:
  # Catalog configuration
  catalog:
    type: "json"
    uri: "file://./my-lakehouse/catalog.json"
  
  config:
    type: "memory"`

	// Write config to temporary file
	tmpFile, err := os.CreateTemp("", "icebox-config-*.yml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write config content: %v", err)
	}

	// Load the configuration
	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify the configuration was loaded correctly
	if cfg.Storage.Catalog.Type != "json" {
		t.Errorf("Expected catalog type 'json', got '%s'", cfg.Storage.Catalog.Type)
	}

	if cfg.Storage.Catalog.URI != "file://./my-lakehouse/catalog.json" {
		t.Errorf("Expected catalog URI 'file://./my-lakehouse/catalog.json', got '%s'", cfg.Storage.Catalog.URI)
	}

	if cfg.Storage.Config.Type != "memory" {
		t.Errorf("Expected storage type 'memory', got '%s'", cfg.Storage.Config.Type)
	}

	if cfg.Storage.Config.Path != "" {
		t.Errorf("Expected empty path for memory storage, got '%s'", cfg.Storage.Config.Path)
	}
}

func TestLoadConfigWithFilesystem(t *testing.T) {
	// Create a temporary config file with filesystem storage
	configContent := `# Icebox Server Configuration
version: "0.1.0"

# Logging Configuration
log:
  level: "info"
  format: "console"
  file_path: "logs/icebox-server.log"
  console: true
  max_size: 100
  max_backups: 3
  max_age: 7
  cleanup: true

# Storage Configuration
storage:
  # Catalog configuration
  catalog:
    type: "sqlite"
    uri: "file://./my-lakehouse/catalog.db"
  
  config:
    type: "filesystem"
    path: "file://./my-lakehouse"`

	// Write config to temporary file
	tmpFile, err := os.CreateTemp("", "icebox-config-*.yml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write config content: %v", err)
	}

	// Load the configuration
	cfg, err := LoadConfig(tmpFile.Name())
	if err != nil {
		t.Fatalf("Failed to load config: %v", err)
	}

	// Verify the configuration was loaded correctly
	if cfg.Storage.Catalog.Type != "sqlite" {
		t.Errorf("Expected catalog type 'sqlite', got '%s'", cfg.Storage.Catalog.Type)
	}

	if cfg.Storage.Config.Type != "filesystem" {
		t.Errorf("Expected storage type 'filesystem', got '%s'", cfg.Storage.Config.Type)
	}

	if cfg.Storage.Config.Path != "file://./my-lakehouse" {
		t.Errorf("Expected path 'file://./my-lakehouse', got '%s'", cfg.Storage.Config.Path)
	}
}

func TestLoadConfigValidation(t *testing.T) {
	// Test that validation fails when path is missing for filesystem storage
	configContent := `# Icebox Server Configuration
version: "0.1.0"

# Logging Configuration
log:
  level: "info"
  format: "console"
  file_path: "logs/icebox-server.log"
  console: true
  max_size: 100
  max_backups: 3
  max_age: 7
  cleanup: true

# Storage Configuration
storage:
  # Catalog configuration
  catalog:
    type: "json"
    uri: "file://./my-lakehouse/catalog.json"
  
  config:
    type: "filesystem"`

	// Write config to temporary file
	tmpFile, err := os.CreateTemp("", "icebox-config-*.yml")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile.Name())
	defer tmpFile.Close()

	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("Failed to write config content: %v", err)
	}

	// Load the configuration should fail validation
	_, err = LoadConfig(tmpFile.Name())
	if err == nil {
		t.Fatal("Expected validation to fail for filesystem storage without path")
	}

	expectedError := "path is required for filesystem storage type"
	if !strings.Contains(err.Error(), expectedError) {
		t.Errorf("Expected error to contain '%s', got '%s'", expectedError, err.Error())
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := LoadDefaultConfig()

	// Verify default values
	if cfg.Storage.Config.Type != "memory" {
		t.Errorf("Expected default storage type 'memory', got '%s'", cfg.Storage.Config.Type)
	}

	if cfg.Storage.Config.Path != "" {
		t.Errorf("Expected default storage path to be empty, got '%s'", cfg.Storage.Config.Path)
	}

	if cfg.Storage.Catalog.Type != "file" {
		t.Errorf("Expected default catalog type 'file', got '%s'", cfg.Storage.Catalog.Type)
	}
}
