package parser

import (
	"github.com/TFMV/icebox/server/config"
)

// getTestConfig returns a test configuration
func getTestConfig() *config.Config {
	cfg := config.LoadDefaultConfig()
	cfg.Storage.DataPath = "/tmp/icebox_test"
	cfg.Storage.Data.Type = "memory" // Use memory storage for testing
	return cfg
}

// CreateTestCatalogAdapter creates a test catalog adapter
func CreateTestCatalogAdapter() (CatalogAdapter, error) {
	cfg := getTestConfig()
	return NewIceboxCatalogAdapter(cfg)
}
