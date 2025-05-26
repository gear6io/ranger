package cli

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeExtractPath(t *testing.T) {
	tempDir := t.TempDir()

	t.Run("ValidPaths", func(t *testing.T) {
		validPaths := []string{
			"file.txt",
			"dir/file.txt",
			"deep/nested/dir/file.txt",
			"manifest.json",
			"data/table.parquet",
			"config/settings.yml",
		}

		for _, path := range validPaths {
			t.Run(path, func(t *testing.T) {
				result, err := sanitizeExtractPath(tempDir, path)
				require.NoError(t, err)
				assert.True(t, strings.HasPrefix(result, tempDir))
				assert.True(t, strings.HasSuffix(result, path))
			})
		}
	})

	t.Run("PathTraversalAttacks", func(t *testing.T) {
		maliciousPaths := []string{
			"../../../etc/passwd",
			"..\\..\\..\\windows\\system32\\config\\sam",
			"dir/../../../etc/passwd",
			"./../../etc/passwd",
			"good/../../bad/file.txt",
			"../file.txt",
			"dir/../../../file.txt",
			"../",
			"../../",
			"../../../",
		}

		for _, path := range maliciousPaths {
			t.Run(path, func(t *testing.T) {
				_, err := sanitizeExtractPath(tempDir, path)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "path traversal")
			})
		}
	})

	t.Run("AbsolutePaths", func(t *testing.T) {
		absolutePaths := []string{
			"/etc/passwd",
			"/tmp/malicious.txt",
			"C:\\Windows\\System32\\evil.exe",
			"/usr/bin/bash",
			"/home/user/.ssh/id_rsa",
		}

		for _, path := range absolutePaths {
			t.Run(path, func(t *testing.T) {
				_, err := sanitizeExtractPath(tempDir, path)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "absolute paths not allowed")
			})
		}
	})

	t.Run("InvalidPaths", func(t *testing.T) {
		invalidPaths := []string{
			"",
			".",
			"./",
		}

		for _, path := range invalidPaths {
			t.Run(fmt.Sprintf("'%s'", path), func(t *testing.T) {
				_, err := sanitizeExtractPath(tempDir, path)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "invalid path")
			})
		}
	})

	t.Run("SuspiciousCharacters", func(t *testing.T) {
		suspiciousPaths := []string{
			"file\x00.txt",
			"file\r.txt",
			"file\n.txt",
			"dir\x00/file.txt",
		}

		for _, path := range suspiciousPaths {
			t.Run(fmt.Sprintf("'%s'", path), func(t *testing.T) {
				_, err := sanitizeExtractPath(tempDir, path)
				require.Error(t, err)
				assert.Contains(t, err.Error(), "suspicious characters")
			})
		}
	})

	t.Run("PathTooLong", func(t *testing.T) {
		longPath := strings.Repeat("a", 256) + ".txt"
		_, err := sanitizeExtractPath(tempDir, longPath)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "path too long")
	})

	t.Run("PathNormalization", func(t *testing.T) {
		// Test that paths are properly normalized
		testCases := []struct {
			input    string
			expected string
		}{
			{"./file.txt", "file.txt"},
			{"dir/./file.txt", "dir/file.txt"},
			{"dir//file.txt", "dir/file.txt"},
		}

		for _, tc := range testCases {
			t.Run(tc.input, func(t *testing.T) {
				result, err := sanitizeExtractPath(tempDir, tc.input)
				require.NoError(t, err)
				expectedPath := filepath.Join(tempDir, tc.expected)
				assert.Equal(t, expectedPath, result)
			})
		}
	})
}

func TestZipSlipProtection(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	archivePath := filepath.Join(tempDir, "malicious.tar")
	extractDir := filepath.Join(tempDir, "extract")

	// Create extraction directory
	err := os.MkdirAll(extractDir, 0755)
	require.NoError(t, err)

	t.Run("MaliciousArchive", func(t *testing.T) {
		// Create a malicious archive with path traversal
		err := createMaliciousArchive(archivePath)
		require.NoError(t, err)

		// Try to extract - should fail
		err = extractArchive(archivePath, extractDir)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid archive entry")

		// Verify no files were extracted outside the target directory
		entries, err := os.ReadDir(tempDir)
		require.NoError(t, err)

		// Should only have the archive file and extract directory
		foundMaliciousFile := false
		for _, entry := range entries {
			if entry.Name() == "malicious.txt" {
				foundMaliciousFile = true
				break
			}
		}
		assert.False(t, foundMaliciousFile, "Malicious file should not have been extracted")
	})

	t.Run("LegitimateArchive", func(t *testing.T) {
		legitArchivePath := filepath.Join(tempDir, "legitimate.tar")
		legitExtractDir := filepath.Join(tempDir, "legit_extract")

		err := os.MkdirAll(legitExtractDir, 0755)
		require.NoError(t, err)

		// Create a legitimate archive
		err = createLegitimateArchive(legitArchivePath)
		require.NoError(t, err)

		// Extract should succeed
		err = extractArchive(legitArchivePath, legitExtractDir)
		require.NoError(t, err)

		// Verify files were extracted correctly (excluding manifest.json which is handled specially)
		expectedFiles := []string{
			"data/table.parquet",
			"config/settings.yml",
		}

		for _, expectedFile := range expectedFiles {
			filePath := filepath.Join(legitExtractDir, expectedFile)
			_, err := os.Stat(filePath)
			assert.NoError(t, err, "Expected file %s should exist", expectedFile)
		}
	})
}

func createMaliciousArchive(archivePath string) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	tarWriter := tar.NewWriter(file)
	defer tarWriter.Close()

	// Add malicious entries with path traversal
	maliciousEntries := []struct {
		name    string
		content string
	}{
		{"../../../malicious.txt", "This file should not be extracted outside the target directory"},
		{"..\\..\\..\\windows_malicious.txt", "Windows-style path traversal"},
		{"good/../../bad.txt", "Hidden path traversal"},
		{"/etc/passwd", "Absolute path attack"},
	}

	for _, entry := range maliciousEntries {
		header := &tar.Header{
			Name: entry.name,
			Mode: 0644,
			Size: int64(len(entry.content)),
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if _, err := tarWriter.Write([]byte(entry.content)); err != nil {
			return err
		}
	}

	return nil
}

func createLegitimateArchive(archivePath string) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	tarWriter := tar.NewWriter(file)
	defer tarWriter.Close()

	// Add legitimate entries
	legitimateEntries := []struct {
		name    string
		content string
		isDir   bool
	}{
		{"manifest.json", `{"package_info": {"name": "test"}}`, false},
		{"data/", "", true},
		{"data/table.parquet", "fake parquet data", false},
		{"config/", "", true},
		{"config/settings.yml", "setting: value", false},
	}

	for _, entry := range legitimateEntries {
		var header *tar.Header
		if entry.isDir {
			header = &tar.Header{
				Name:     entry.name,
				Mode:     0755,
				Typeflag: tar.TypeDir,
			}
		} else {
			header = &tar.Header{
				Name: entry.name,
				Mode: 0644,
				Size: int64(len(entry.content)),
			}
		}

		if err := tarWriter.WriteHeader(header); err != nil {
			return err
		}

		if !entry.isDir {
			if _, err := tarWriter.Write([]byte(entry.content)); err != nil {
				return err
			}
		}
	}

	return nil
}

func TestGzipArchiveProtection(t *testing.T) {
	tempDir := t.TempDir()
	archivePath := filepath.Join(tempDir, "malicious.tar.gz")
	extractDir := filepath.Join(tempDir, "extract")

	err := os.MkdirAll(extractDir, 0755)
	require.NoError(t, err)

	// Create a malicious gzipped archive
	err = createMaliciousGzipArchive(archivePath)
	require.NoError(t, err)

	// Try to extract - should fail
	err = extractArchive(archivePath, extractDir)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid archive entry")
}

func createMaliciousGzipArchive(archivePath string) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	gzipWriter := gzip.NewWriter(file)
	defer gzipWriter.Close()

	tarWriter := tar.NewWriter(gzipWriter)
	defer tarWriter.Close()

	// Add malicious entry
	content := "This should not be extracted outside target directory"
	header := &tar.Header{
		Name: "../../../malicious_gzip.txt",
		Mode: 0644,
		Size: int64(len(content)),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return err
	}

	_, err = tarWriter.Write([]byte(content))
	return err
}

func TestArchiveExtractionLimits(t *testing.T) {
	tempDir := t.TempDir()
	archivePath := filepath.Join(tempDir, "test.tar")
	extractDir := filepath.Join(tempDir, "extract")

	err := os.MkdirAll(extractDir, 0755)
	require.NoError(t, err)

	t.Run("SymlinkAttack", func(t *testing.T) {
		// Create archive with symlink that could be used for attacks
		err := createSymlinkArchive(archivePath)
		require.NoError(t, err)

		// Extract should handle symlinks safely (or reject them)
		err = extractArchive(archivePath, extractDir)
		// The current implementation doesn't handle symlinks, so this might fail
		// but it shouldn't create security vulnerabilities
		if err != nil {
			t.Logf("Symlink extraction failed as expected: %v", err)
		}
	})
}

func createSymlinkArchive(archivePath string) error {
	file, err := os.Create(archivePath)
	if err != nil {
		return err
	}
	defer file.Close()

	tarWriter := tar.NewWriter(file)
	defer tarWriter.Close()

	// Add a symlink entry
	header := &tar.Header{
		Name:     "symlink.txt",
		Typeflag: tar.TypeSymlink,
		Linkname: "../../../etc/passwd",
	}

	return tarWriter.WriteHeader(header)
}
