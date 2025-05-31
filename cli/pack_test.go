package cli

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/display"
	"github.com/rs/zerolog"
)

func TestShouldSkip(t *testing.T) {
	tests := []struct {
		name     string
		relPath  string
		isDir    bool
		expected bool
	}{
		{
			name:     "keep .icebox.yml",
			relPath:  ".icebox.yml",
			expected: false,
		},
		{
			name:     "skip .git directory",
			relPath:  ".git/config",
			expected: true,
		},
		{
			name:     "skip node_modules",
			relPath:  "node_modules/package/index.js",
			expected: true,
		},
		{
			name:     "skip .vscode",
			relPath:  ".vscode/settings.json",
			expected: true,
		},
		{
			name:     "keep regular files",
			relPath:  "src/main.go",
			expected: false,
		},
		{
			name:     "keep .icebox directory files",
			relPath:  ".icebox/catalog/catalog.db",
			expected: false,
		},
		{
			name:     "skip other hidden files",
			relPath:  ".env",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &mockFileInfo{isDir: tt.isDir}
			result := shouldSkip(tt.relPath, info)
			if result != tt.expected {
				t.Errorf("shouldSkip(%s) = %v, want %v", tt.relPath, result, tt.expected)
			}
		})
	}
}

func TestIsDataFile(t *testing.T) {
	tests := []struct {
		name     string
		relPath  string
		expected bool
	}{
		{
			name:     "parquet file in data directory",
			relPath:  ".icebox/data/default/sales/data.parquet",
			expected: true,
		},
		{
			name:     "parquet file elsewhere",
			relPath:  "imports/data.parquet",
			expected: true,
		},
		{
			name:     "avro file",
			relPath:  "data/events.avro",
			expected: true,
		},
		{
			name:     "config file",
			relPath:  ".icebox.yml",
			expected: false,
		},
		{
			name:     "catalog database",
			relPath:  ".icebox/catalog/catalog.db",
			expected: false,
		},
		{
			name:     "regular file",
			relPath:  "README.md",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDataFile(tt.relPath)
			if result != tt.expected {
				t.Errorf("isDataFile(%s) = %v, want %v", tt.relPath, result, tt.expected)
			}
		})
	}
}

func TestPackageManifestCreation(t *testing.T) {
	cfg := &config.Config{
		Name: "test-project",
	}

	manifest := &PackageManifest{
		PackageInfo: PackageInfo{
			Name:        cfg.Name,
			Version:     "0.1.0",
			CreatedBy:   "icebox",
			IncludeData: false,
		},
		Files:  make(map[string]FileInfo),
		Config: cfg,
	}

	// Test manifest structure
	if manifest.PackageInfo.Name != "test-project" {
		t.Errorf("Expected project name 'test-project', got %s", manifest.PackageInfo.Name)
	}

	if manifest.PackageInfo.Version != "0.1.0" {
		t.Errorf("Expected version '0.1.0', got %s", manifest.PackageInfo.Version)
	}

	if manifest.Files == nil {
		t.Error("Files map should be initialized")
	}
}

func TestPackAndUnpackIntegration(t *testing.T) {
	// Skip integration test unless explicitly enabled
	if os.Getenv("ICEBOX_INTEGRATION_TESTS") != "true" {
		t.Skip("Skipping integration test - set ICEBOX_INTEGRATION_TESTS=true to enable")
	}

	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "icebox-pack-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a test project structure
	projectDir := filepath.Join(tempDir, "test-project")
	if err := os.MkdirAll(projectDir, 0755); err != nil {
		t.Fatalf("Failed to create project dir: %v", err)
	}

	// Create test configuration
	cfg := &config.Config{
		Name: "test-project",
		Catalog: config.CatalogConfig{
			Type: "sqlite",
			SQLite: &config.SQLiteConfig{
				Path: ".icebox/catalog/catalog.db",
			},
		},
		Storage: config.StorageConfig{
			Type: "fs",
			FileSystem: &config.FileSystemConfig{
				RootPath: ".icebox/data",
			},
		},
	}

	// Write config file
	configPath := filepath.Join(projectDir, ".icebox.yml")
	if err := config.WriteConfig(configPath, cfg); err != nil {
		t.Fatalf("Failed to write config: %v", err)
	}

	// Create some test files
	testFiles := map[string]string{
		"README.md":                               "# Test Project\nThis is a test.",
		".icebox/catalog/catalog.db":              "fake database content",
		".icebox/data/default/sales/data.parquet": "fake parquet data",
		"src/main.go":                             "package main\n\nfunc main() {}\n",
	}

	for relPath, content := range testFiles {
		fullPath := filepath.Join(projectDir, relPath)
		if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
			t.Fatalf("Failed to create directory for %s: %v", relPath, err)
		}
		if err := os.WriteFile(fullPath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write file %s: %v", relPath, err)
		}
	}

	// Test packing without data
	archivePath := filepath.Join(tempDir, "test.tar.gz")
	packOpts.includeData = false
	packOpts.checksum = true
	packOpts.compress = true
	packOpts.output = archivePath

	// Create test context and display
	ctx := context.Background()
	d := display.New()
	logger := zerolog.Nop() // No-op logger for tests

	if err := createArchive(ctx, projectDir, archivePath, cfg, d, &logger); err != nil {
		t.Fatalf("Failed to create archive: %v", err)
	}

	// Verify archive was created
	if _, err := os.Stat(archivePath); os.IsNotExist(err) {
		t.Fatal("Archive file was not created")
	}

	// Test extracting
	extractDir := filepath.Join(tempDir, "extracted")
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		t.Fatalf("Failed to create extract dir: %v", err)
	}

	unpackOpts.verify = true
	unpackOpts.overwrite = true
	unpackOpts.skipData = false

	if err := extractArchive(ctx, archivePath, extractDir, d, &logger); err != nil {
		t.Fatalf("Failed to extract archive: %v", err)
	}

	// Verify extracted files
	expectedFiles := []string{
		".icebox.yml",
		"README.md",
		".icebox/catalog/catalog.db",
		"src/main.go",
		"manifest.json",
	}

	for _, relPath := range expectedFiles {
		fullPath := filepath.Join(extractDir, relPath)
		if _, err := os.Stat(fullPath); os.IsNotExist(err) {
			t.Errorf("Expected file %s was not extracted", relPath)
		}
	}

	// Verify data file was excluded (since includeData was false)
	dataPath := filepath.Join(extractDir, ".icebox/data/default/sales/data.parquet")
	if _, err := os.Stat(dataPath); !os.IsNotExist(err) {
		t.Error("Data file should have been excluded but was found")
	}

	// Verify manifest was created and is valid
	manifestPath := filepath.Join(extractDir, "manifest.json")
	manifestData, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}

	var manifest PackageManifest
	if err := json.Unmarshal(manifestData, &manifest); err != nil {
		t.Fatalf("Failed to parse manifest: %v", err)
	}

	if manifest.PackageInfo.Name != "test-project" {
		t.Errorf("Expected manifest project name 'test-project', got %s", manifest.PackageInfo.Name)
	}

	if len(manifest.Files) == 0 {
		t.Error("Manifest should contain file information")
	}
}

func TestArchiveReading(t *testing.T) {
	// Test that we can read a compressed archive
	tempDir, err := os.MkdirTemp("", "icebox-read-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a simple test archive
	archivePath := filepath.Join(tempDir, "test.tar.gz")
	file, err := os.Create(archivePath)
	if err != nil {
		t.Fatalf("Failed to create archive file: %v", err)
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	tarWriter := tar.NewWriter(gzipWriter)

	// Add a test file
	testContent := "test file content"
	header := &tar.Header{
		Name: "test.txt",
		Mode: 0644,
		Size: int64(len(testContent)),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		t.Fatalf("Failed to write tar header: %v", err)
	}

	if _, err := tarWriter.Write([]byte(testContent)); err != nil {
		t.Fatalf("Failed to write tar content: %v", err)
	}

	tarWriter.Close()
	gzipWriter.Close()

	// Test reading the archive
	extractDir := filepath.Join(tempDir, "extracted")
	if err := os.MkdirAll(extractDir, 0755); err != nil {
		t.Fatalf("Failed to create extract dir: %v", err)
	}

	unpackOpts.verify = false
	unpackOpts.overwrite = true

	// Create test context and display
	ctx := context.Background()
	d := display.New()
	logger := zerolog.Nop() // No-op logger for tests

	if err := extractArchive(ctx, archivePath, extractDir, d, &logger); err != nil {
		t.Fatalf("Failed to extract archive: %v", err)
	}

	// Verify the file was extracted
	extractedPath := filepath.Join(extractDir, "test.txt")
	content, err := os.ReadFile(extractedPath)
	if err != nil {
		t.Fatalf("Failed to read extracted file: %v", err)
	}

	if string(content) != testContent {
		t.Errorf("Expected content '%s', got '%s'", testContent, string(content))
	}
}

// Mock file info for testing
type mockFileInfo struct {
	name  string
	size  int64
	mode  os.FileMode
	isDir bool
}

func (m *mockFileInfo) Name() string       { return m.name }
func (m *mockFileInfo) Size() int64        { return m.size }
func (m *mockFileInfo) Mode() os.FileMode  { return m.mode }
func (m *mockFileInfo) ModTime() time.Time { return time.Time{} }
func (m *mockFileInfo) IsDir() bool        { return m.isDir }
func (m *mockFileInfo) Sys() interface{}   { return nil }

func TestPackOptions(t *testing.T) {
	// Test default pack options
	defaultOpts := &packOptions{}

	if defaultOpts.includeData {
		t.Error("includeData should default to false")
	}

	if defaultOpts.checksum {
		t.Error("checksum should default to false (set by flag)")
	}

	if defaultOpts.maxSize != 0 {
		t.Error("maxSize should default to 0 (set by flag)")
	}
}

func TestUnpackOptions(t *testing.T) {
	// Test default unpack options
	defaultOpts := &unpackOptions{}

	if defaultOpts.verify {
		t.Error("verify should default to false (set by flag)")
	}

	if defaultOpts.overwrite {
		t.Error("overwrite should default to false")
	}

	if defaultOpts.skipData {
		t.Error("skipData should default to false")
	}
}

// Benchmark tests for performance validation
func BenchmarkShouldSkip(b *testing.B) {
	info := &mockFileInfo{isDir: false}
	path := "src/main.go"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		shouldSkip(path, info)
	}
}

func BenchmarkIsDataFile(b *testing.B) {
	path := ".icebox/data/default/sales/data.parquet"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		isDataFile(path)
	}
}
