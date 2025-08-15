package parser

import (
	"github.com/TFMV/icebox/server/config"
)

// CreateTestConfig creates a test configuration for catalog testing
func CreateTestConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Storage.Catalog.Type = "json"
	cfg.Storage.Path = "/tmp/icebox_test"
	return cfg
}

// CreateTestCatalogAdapter creates a test catalog adapter
func CreateTestCatalogAdapter() (CatalogAdapter, error) {
	cfg := CreateTestConfig()
	return NewIceboxCatalogAdapter(cfg)
}
