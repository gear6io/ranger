package storage

import (
	"path/filepath"
	"testing"
)

func TestPathManager(t *testing.T) {
	// Test with a simple path
	pm := NewPathManager("./data")

	// Test base path
	if pm.GetBasePath() != "./data" {
		t.Errorf("Expected base path './data', got '%s'", pm.GetBasePath())
	}

	// Test catalog path
	expectedCatalogPath := filepath.Join("./data", "catalog")
	if pm.GetCatalogPath() != expectedCatalogPath {
		t.Errorf("Expected catalog path '%s', got '%s'", expectedCatalogPath, pm.GetCatalogPath())
	}

	// Test data path
	expectedDataPath := filepath.Join("./data", "data")
	if pm.GetDataPath() != expectedDataPath {
		t.Errorf("Expected data path '%s', got '%s'", expectedDataPath, pm.GetDataPath())
	}

	// Test internal metadata path
	expectedMetadataPath := filepath.Join("./data", ".icebox")
	if pm.GetInternalMetadataPath() != expectedMetadataPath {
		t.Errorf("Expected metadata path '%s', got '%s'", expectedMetadataPath, pm.GetInternalMetadataPath())
	}

	// Test table path
	expectedTablePath := filepath.Join("./data", "data", "default", "users")
	if pm.GetTablePath("default", "users") != expectedTablePath {
		t.Errorf("Expected table path '%s', got '%s'", expectedTablePath, pm.GetTablePath("default", "users"))
	}
}

func TestParseTableIdentifier(t *testing.T) {
	pm := NewPathManager("/test/path")

	tests := []struct {
		input     string
		database  string
		tableName string
	}{
		{"users", "default", "users"},
		{"analytics.users", "analytics", "users"},
		{"db.table", "db", "table"},
		{"schema.users", "schema", "users"},
		{"", "default", ""},
	}

	for _, test := range tests {
		database, tableName := pm.ParseTableIdentifier(test.input)
		if database != test.database || tableName != test.tableName {
			t.Errorf("ParseTableIdentifier(%q) = (%q, %q), want (%q, %q)",
				test.input, database, tableName, test.database, test.tableName)
		}
	}
}

func TestGetCatalogURI(t *testing.T) {
	pm := NewPathManager("/test/path")

	tests := []struct {
		catalogType string
		expected    string
	}{
		{"json", "file:///test/path/catalog/catalog.json"},
		{"sqlite", "file:///test/path/catalog/catalog.db"},
		{"rest", ""},
		{"", ""},
	}

	for _, test := range tests {
		result := pm.GetCatalogURI(test.catalogType)
		if result != test.expected {
			t.Errorf("GetCatalogURI(%q) = %q, want %q", test.catalogType, result, test.expected)
		}
	}

	// Test with empty base path
	pmEmpty := NewPathManager("")
	result := pmEmpty.GetCatalogURI("json")
	if result != "" {
		t.Errorf("GetCatalogURI with empty base path should return empty string, got %q", result)
	}
}
