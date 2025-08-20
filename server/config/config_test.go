package config

import (
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := LoadDefaultConfig()

	// Test that data_path is set by default
	if cfg.GetStoragePath() != "./data" {
		t.Errorf("Expected default data_path to be './data', got '%s'", cfg.GetStoragePath())
	}

	// Test that storage type is set by default
	if cfg.GetStorageType() != "filesystem" {
		t.Errorf("Expected default storage type to be 'filesystem', got '%s'", cfg.GetStorageType())
	}

	// Test that catalog type is set by default
	if cfg.GetCatalogType() != "json" {
		t.Errorf("Expected default catalog type to be 'json', got '%s'", cfg.GetCatalogType())
	}

	// Test that metadata is not configurable (always SQLite)
	// This is now handled internally by PathManager
}

func TestConfigValidation(t *testing.T) {
	cfg := LoadDefaultConfig()

	// Test that default config validates
	if err := cfg.Validate(); err != nil {
		t.Errorf("Default config should validate, got error: %v", err)
	}

	// Test that empty data_path fails validation
	cfg.Storage.DataPath = ""
	if err := cfg.Validate(); err == nil {
		t.Error("Config with empty data_path should fail validation")
	}
}
