package storage

import (
	"encoding/json"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/TFMV/icebox/server/paths"
	"github.com/TFMV/icebox/server/storage/filesystem"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamingInsertData(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "streaming_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create path manager
	pathManager := paths.NewManager(tempDir)

	// Create filesystem storage
	fs := filesystem.NewFileStorage(pathManager)

	// Test streaming write directly
	writer, err := fs.OpenTableForWrite("testdb", "users")
	require.NoError(t, err)

	// Test data
	testData := [][]interface{}{
		{1, "Alice", 25.5},
		{2, "Bob", 30.2},
		{3, "Charlie", 28.7},
		{4, "Diana", 35.1},
		{5, "Eve", 27.9},
	}

	// Write data in smaller batches to test batching logic
	batchSize := 1 // Write each row as a separate batch to test the batching mechanism
	for i := 0; i < len(testData); i += batchSize {
		end := i + batchSize
		if end > len(testData) {
			end = len(testData)
		}

		batch := testData[i:end]

		// Convert batch to JSON and write directly to storage
		batchBytes, err := json.Marshal(batch)
		require.NoError(t, err)

		// Write batch directly to storage without intermediate buffering
		_, err = writer.Write(batchBytes)
		require.NoError(t, err)

		// Add newline separator between batches for readability
		_, err = writer.Write([]byte("\n"))
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Verify data was written by reading it back
	reader, err := fs.OpenTableForRead("testdb", "users")
	require.NoError(t, err)
	defer reader.Close()

	content, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Verify the content contains our data in the expected format
	// Each batch should be on a separate line
	lines := strings.Split(string(content), "\n")
	// Remove empty lines
	var nonEmptyLines []string
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			nonEmptyLines = append(nonEmptyLines, line)
		}
	}

	// Should have 5 lines (one for each row since batchSize=1000)
	assert.Equal(t, 5, len(nonEmptyLines))

	// Verify each line is valid JSON
	for i, line := range nonEmptyLines {
		var batchData [][]interface{}
		err := json.Unmarshal([]byte(line), &batchData)
		require.NoError(t, err, "Line %d should be valid JSON: %s", i, line)
		assert.Equal(t, 1, len(batchData), "Each batch should have 1 row")
		t.Logf("Batch %d: %v", i, batchData)
	}
}

func TestStreamingGetTableData(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "streaming_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create path manager
	pathManager := paths.NewManager(tempDir)

	// Create actual filesystem storage using the proper constructor
	fsEngine := filesystem.NewFileStorage(pathManager)

	// First, write some test data directly
	writer, err := fsEngine.OpenTableForWrite("testdb", "users")
	require.NoError(t, err)

	testData := [][]interface{}{
		{1, "Alice", 25.5},
		{2, "Bob", 30.2},
		{3, "Charlie", 28.7},
	}

	// Write each row as a separate batch
	for _, row := range testData {
		batchBytes, err := json.Marshal([][]interface{}{row})
		require.NoError(t, err)

		_, err = writer.Write(batchBytes)
		require.NoError(t, err)
		_, err = writer.Write([]byte("\n"))
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Now test streaming read
	reader, err := fsEngine.OpenTableForRead("testdb", "users")
	require.NoError(t, err)
	defer reader.Close()

	// Read all content
	content, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Parse the content line by line
	lines := strings.Split(string(content), "\n")
	var allData [][]interface{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		var batch [][]interface{}
		err := json.Unmarshal([]byte(line), &batch)
		require.NoError(t, err)
		allData = append(allData, batch...)
	}

	assert.Equal(t, 3, len(allData))

	// Verify data content (JSON unmarshaling converts integers to float64)
	expected := [][]interface{}{
		{float64(1), "Alice", 25.5},
		{float64(2), "Bob", 30.2},
		{float64(3), "Charlie", 28.7},
	}

	for i, row := range allData {
		assert.Equal(t, expected[i], row)
	}
}

// Test the actual streaming writer and reader implementations
func TestStreamingWriterReader(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "streaming_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create path manager
	pathManager := paths.NewManager(tempDir)

	// Create actual filesystem storage using the proper constructor
	fsEngine := filesystem.NewFileStorage(pathManager)

	// Test streaming write
	writer, err := fsEngine.OpenTableForWrite("testdb", "streaming_test")
	require.NoError(t, err)

	// Write data in chunks
	testChunks := [][]byte{
		[]byte(`[{"id": 1, "name": "Alice"}]`),
		[]byte(`[{"id": 2, "name": "Bob"}]`),
		[]byte(`[{"id": 3, "name": "Charlie"}]`),
	}

	for _, chunk := range testChunks {
		_, err := writer.Write(chunk)
		require.NoError(t, err)
		// Add newline separator
		_, err = writer.Write([]byte("\n"))
		require.NoError(t, err)
	}

	err = writer.Close()
	require.NoError(t, err)

	// Test streaming read
	reader, err := fsEngine.OpenTableForRead("testdb", "streaming_test")
	require.NoError(t, err)
	defer reader.Close()

	// Read all content
	content, err := io.ReadAll(reader)
	require.NoError(t, err)

	// Verify content
	expectedContent := `[{"id": 1, "name": "Alice"}]
[{"id": 2, "name": "Bob"}]
[{"id": 3, "name": "Charlie"}]
`
	assert.Equal(t, expectedContent, string(content))
}
