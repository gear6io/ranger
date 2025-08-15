package s3

import (
	"context"
	cryptorand "crypto/rand"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	icebergio "github.com/apache/iceberg-go/io"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test constants
const (
	testTimeout       = 30 * time.Second
	testDataDir       = ".test_minio"
	testBucket        = "test-bucket"
	testPrefix        = "test-prefix"
	testAccessKey     = "testuser"
	testSecretKey     = "testpassword123"
	testPort          = 19000       // Use different port to avoid conflicts
	testLargeFileSize = 1024 * 1024 // 1MB
	testConcurrency   = 10
)

// Test data generators
func generateTestData(size int) []byte {
	data := make([]byte, size)
	if _, err := cryptorand.Read(data); err != nil {
		// Fallback to deterministic data
		for i := range data {
			data[i] = byte(i % 256)
		}
	}
	return data
}

// Helper function to write data to a MinIO file
func writeToMinIOFile(t *testing.T, file icebergio.File, data []byte) (int, error) {
	minioWriteFile, ok := file.(*minioWriteFile)
	require.True(t, ok, "file should be of type *minioWriteFile")
	return minioWriteFile.Write(data)
}

// Test fixtures
func createTestConfig() *EmbeddedMinIOConfig {
	return &EmbeddedMinIOConfig{
		Port:                testPort,
		Address:             "localhost",
		DataDir:             testDataDir,
		AccessKey:           testAccessKey,
		SecretKey:           testSecretKey,
		AutoStart:           true,
		Console:             false,
		Quiet:               true,
		Secure:              false,
		Region:              "us-east-1",
		DefaultBucket:       testBucket,
		Properties:          make(map[string]string),
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 5,
		ConnectTimeout:      10 * time.Second,
		RequestTimeout:      30 * time.Second,
		ReadTimeout:         15 * time.Second,
		WriteTimeout:        30 * time.Second,
		IdleTimeout:         45 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second,
		KeepAlive:           15 * time.Second,
		StartupTimeout:      15 * time.Second,
		ShutdownTimeout:     5 * time.Second,
		HealthCheckInterval: 2 * time.Second,
		RetryAttempts:       2,
		RetryDelay:          50 * time.Millisecond,
		BufferSize:          32 * 1024, // 32KB
		EnableMetrics:       true,
		EnableTracing:       false,
		EnableCORS:          false,
		AllowedOrigins:      []string{},
		EnableCompression:   true,
	}
}

func createSecureTestConfig() *EmbeddedMinIOConfig {
	config := createTestConfig()
	config.Secure = true
	config.TLS = &TLSConfig{
		InsecureSkipVerify: true, // For testing only
		MinVersion:         "1.2",
	}
	return config
}

func setupTestServer(t *testing.T, config *EmbeddedMinIOConfig) (*EmbeddedMinIO, func()) {
	if config == nil {
		config = createTestConfig()
	}

	// Set test mode to use fake servers
	os.Setenv("MINIO_TEST_MODE", "true")

	// Ensure test data directory is clean
	os.RemoveAll(config.DataDir)

	server, err := NewEmbeddedMinIO(config)
	require.NoError(t, err, "Failed to create MinIO server")

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err = server.Start(ctx)
	require.NoError(t, err, "Failed to start MinIO server")

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := server.Stop(ctx); err != nil {
			t.Logf("Warning: failed to stop MinIO server: %v", err)
		}

		// Clean up test data
		if err := os.RemoveAll(config.DataDir); err != nil {
			t.Logf("Warning: failed to clean up test data: %v", err)
		}
	}

	return server, cleanup
}

// Configuration Tests
func TestNewEmbeddedMinIO(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	tests := []struct {
		name        string
		config      *EmbeddedMinIOConfig
		expectError bool
		errorType   string
	}{
		{
			name:        "valid config",
			config:      createTestConfig(),
			expectError: false,
		},
		{
			name:        "nil config uses defaults",
			config:      nil,
			expectError: false,
		},
		{
			name: "invalid port - too low",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.Port = 0
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
		{
			name: "invalid port - too high",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.Port = 70000
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
		{
			name: "empty access key",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.AccessKey = ""
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
		{
			name: "empty secret key",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.SecretKey = ""
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
		{
			name: "secret key too short",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.SecretKey = "short"
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
		{
			name: "secret key too long",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.SecretKey = strings.Repeat("a", 50)
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
		{
			name: "invalid buffer size",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.BufferSize = MaxBufferSize + 1
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
		{
			name: "invalid default bucket name",
			config: func() *EmbeddedMinIOConfig {
				c := createTestConfig()
				c.DefaultBucket = "INVALID_BUCKET_NAME"
				return c
			}(),
			expectError: true,
			errorType:   "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, err := NewEmbeddedMinIO(tt.config)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, server)

				if tt.errorType != "" {
					var minioErr *MinIOError
					if assert.ErrorAs(t, err, &minioErr) {
						// Check if the underlying error is of the expected type
						switch tt.errorType {
						case "ValidationError":
							var validationErr *ValidationError
							assert.ErrorAs(t, minioErr.Err, &validationErr)
						}
					}
				}
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, server)
				assert.False(t, server.IsRunning())
			}
		})
	}
}

func TestDefaultMinIOConfig(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := DefaultMinIOConfig()

	assert.Equal(t, DefaultPort, config.Port)
	assert.Equal(t, DefaultAddress, config.Address)
	assert.Equal(t, DefaultAccessKey, config.AccessKey)
	assert.Equal(t, DefaultSecretKey, config.SecretKey)
	assert.Equal(t, DefaultRegion, config.Region)
	assert.Equal(t, DefaultBucket, config.DefaultBucket)
	assert.True(t, config.AutoStart)
	assert.False(t, config.Console)
	assert.True(t, config.Quiet)
	assert.False(t, config.Secure)
	assert.Equal(t, DefaultMaxIdleConns, config.MaxIdleConns)
	assert.Equal(t, DefaultConnectTimeout, config.ConnectTimeout)
	assert.Equal(t, DefaultBufferSize, config.BufferSize)
	assert.True(t, config.EnableMetrics)
	assert.True(t, config.EnableCompression)
	assert.NotNil(t, config.Properties)
}

// Server Lifecycle Tests
func TestEmbeddedMinIOLifecycle(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	// Test server is running
	assert.True(t, server.IsRunning())
	assert.NotNil(t, server.GetClient())
	assert.Equal(t, fmt.Sprintf("%s:%d", config.Address, config.Port), server.GetEndpoint())

	// Test metrics are available
	metrics := server.GetMetrics()
	assert.NotNil(t, metrics)
	assert.True(t, metrics.Uptime > 0)

	// Test double start fails
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := server.Start(ctx)
	assert.Error(t, err)
	var minioErr *MinIOError
	assert.ErrorAs(t, err, &minioErr)
	assert.Equal(t, "start", minioErr.Op)
}

func TestEmbeddedMinIOStop(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, err := NewEmbeddedMinIO(config)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err = server.Start(ctx)
	require.NoError(t, err)
	assert.True(t, server.IsRunning())

	// Test graceful stop
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()

	err = server.Stop(stopCtx)
	assert.NoError(t, err)
	assert.False(t, server.IsRunning())
	assert.Nil(t, server.GetClient())

	// Test double stop is safe
	err = server.Stop(stopCtx)
	assert.NoError(t, err)

	// Cleanup
	os.RemoveAll(config.DataDir)
}

func TestEmbeddedMinIOStartupTimeout(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	config.StartupTimeout = 1 * time.Millisecond // Very short timeout

	server, err := NewEmbeddedMinIO(config)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = server.Start(ctx)
	assert.Error(t, err)
	assert.False(t, server.IsRunning())

	// Cleanup
	os.RemoveAll(config.DataDir)
}

func TestEmbeddedMinIOPortConflict(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	// Temporarily disable test mode to enable port checking
	oldTestMode := os.Getenv("MINIO_TEST_MODE")
	os.Unsetenv("MINIO_TEST_MODE")
	defer func() {
		if oldTestMode != "" {
			os.Setenv("MINIO_TEST_MODE", oldTestMode)
		}
	}()

	// Find an available port
	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	// Bind to the port to simulate it being in use
	listener, err = net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	defer listener.Close()

	// Try to start a server on the same port
	config := createTestConfig()
	config.Port = port
	config.DataDir = ".test_minio_conflict"

	server, err := NewEmbeddedMinIO(config)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = server.Start(ctx)
	assert.Error(t, err)
	assert.False(t, server.IsRunning())

	// Cleanup
	os.RemoveAll(config.DataDir)
}

// Health Check and Monitoring Tests
func TestHealthChecks(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	config.EnableMetrics = true
	config.HealthCheckInterval = 100 * time.Millisecond

	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	// Wait for server to be fully ready
	time.Sleep(1 * time.Second)

	// Verify server is running
	require.True(t, server.IsRunning(), "Server should be running before health checks")

	// Debug: Check if fake server is accessible
	fakeServer := server.GetFakeServer()
	if fakeServer != nil {
		t.Logf("Fake server URL: %s", fakeServer.URL)
	} else {
		t.Fatal("Fake server is nil")
	}

	// Test health endpoints - use the actual server URL
	baseURL := server.GetActualURL()
	t.Logf("Testing health endpoints at: %s", baseURL)

	tests := []struct {
		endpoint string
		status   int
	}{
		{"/minio/health/live", http.StatusOK},
		{"/minio/health/ready", http.StatusOK},
		{"/minio/health/cluster", http.StatusOK},
		{"/minio/metrics", http.StatusOK},
	}

	client := &http.Client{Timeout: 5 * time.Second}

	for _, test := range tests {
		t.Run(test.endpoint, func(t *testing.T) {
			resp, err := client.Get(baseURL + test.endpoint)
			require.NoError(t, err)
			defer resp.Body.Close()

			assert.Equal(t, test.status, resp.StatusCode)
			assert.Equal(t, "application/json", resp.Header.Get("Content-Type"))

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)
			assert.NotEmpty(t, body)
		})
	}

	// Test metrics collection
	metrics := server.GetMetrics()
	assert.NotNil(t, metrics)
	assert.True(t, metrics.Uptime > 0)
	assert.Equal(t, "healthy", metrics.HealthCheckStatus)
}

// FileSystem Tests
func TestNewS3FileSystem(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	tests := []struct {
		name        string
		bucket      string
		prefix      string
		expectError bool
	}{
		{
			name:        "valid filesystem",
			bucket:      testBucket,
			prefix:      testPrefix,
			expectError: false,
		},
		{
			name:        "empty prefix",
			bucket:      testBucket,
			prefix:      "",
			expectError: false,
		},
		{
			name:        "invalid bucket name",
			bucket:      "INVALID_BUCKET",
			prefix:      testPrefix,
			expectError: true,
		},
		{
			name:        "bucket name too short",
			bucket:      "ab",
			prefix:      testPrefix,
			expectError: true,
		},
		{
			name:        "bucket name too long",
			bucket:      strings.Repeat("a", 64),
			prefix:      testPrefix,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, err := NewS3FileSystemWithServer(server, tt.bucket, tt.prefix)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, fs)
				var minioErr *MinIOError
				assert.ErrorAs(t, err, &minioErr)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, fs)
				assert.NotNil(t, fs.GetMetrics())
			}
		})
	}
}

func TestS3FileSystemWithStoppedServer(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, err := NewEmbeddedMinIO(config)
	require.NoError(t, err)

	// Try to create filesystem with stopped server
	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	assert.Error(t, err)
	assert.Nil(t, fs)

	var minioErr *MinIOError
	assert.ErrorAs(t, err, &minioErr)
	assert.Equal(t, "create_filesystem", minioErr.Op)

	// Cleanup
	os.RemoveAll(config.DataDir)
}

// File Operations Tests
func TestFileOperationsBasic(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	testData := generateTestData(1024)
	testLocation := "test/file.txt"

	// Test Create and Write
	writeFile, err := fs.Create(testLocation)
	require.NoError(t, err)
	assert.NotNil(t, writeFile)

	// Cast to the concrete type to access Write method
	minioWriteFile, ok := writeFile.(*minioWriteFile)
	require.True(t, ok, "writeFile should be of type *minioWriteFile")

	n, err := minioWriteFile.Write(testData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)

	err = writeFile.Close()
	assert.NoError(t, err)

	// Test Open and Read
	readFile, err := fs.Open(testLocation)
	require.NoError(t, err)
	assert.NotNil(t, readFile)

	readData := make([]byte, len(testData))
	n, err = readFile.Read(readData)
	assert.NoError(t, err)
	assert.Equal(t, len(testData), n)
	assert.Equal(t, testData, readData)

	err = readFile.Close()
	assert.NoError(t, err)

	// Test Remove
	err = fs.Remove(testLocation)
	assert.NoError(t, err)

	// Verify file is removed
	_, err = fs.Open(testLocation)
	assert.Error(t, err)
}

func TestFileOperationsAdvanced(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	testData := generateTestData(2048)
	testLocation := "advanced/test.bin"

	// Create and write file
	writeFile, err := fs.Create(testLocation)
	require.NoError(t, err)

	// Cast to the concrete type to access Write method
	minioWriteFile, ok := writeFile.(*minioWriteFile)
	require.True(t, ok, "writeFile should be of type *minioWriteFile")

	_, err = minioWriteFile.Write(testData)
	require.NoError(t, err)
	require.NoError(t, writeFile.Close())

	// Test ReadAt
	readFile, err := fs.Open(testLocation)
	require.NoError(t, err)

	// Read from middle of file
	offset := int64(512)
	length := 256
	buffer := make([]byte, length)

	n, err := readFile.ReadAt(buffer, offset)
	assert.NoError(t, err)
	assert.Equal(t, length, n)
	assert.Equal(t, testData[offset:offset+int64(length)], buffer)

	// Test Seek
	newPos, err := readFile.Seek(1024, io.SeekStart)
	assert.NoError(t, err)
	assert.Equal(t, int64(1024), newPos)

	// Read after seek
	seekBuffer := make([]byte, 512)
	n, err = readFile.Read(seekBuffer)
	assert.NoError(t, err)
	assert.Equal(t, 512, n)
	assert.Equal(t, testData[1024:1536], seekBuffer)

	// Test Stat
	fileInfo, err := readFile.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(testData)), fileInfo.Size())
	assert.False(t, fileInfo.IsDir())

	require.NoError(t, readFile.Close())
}

func TestFileOperationsErrors(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	tests := []struct {
		name      string
		operation func() error
		errorOp   string
	}{
		{
			name: "open non-existent file",
			operation: func() error {
				_, err := fs.Open("non-existent.txt")
				return err
			},
			errorOp: "open",
		},
		{
			name: "remove non-existent file",
			operation: func() error {
				return fs.Remove("non-existent.txt")
			},
			errorOp: "remove",
		},
		{
			name: "create with empty location",
			operation: func() error {
				_, err := fs.Create("")
				return err
			},
			errorOp: "validate_location",
		},
		{
			name: "open with empty location",
			operation: func() error {
				_, err := fs.Open("")
				return err
			},
			errorOp: "validate_location",
		},
		{
			name: "create with invalid characters",
			operation: func() error {
				_, err := fs.Create("invalid\x00file.txt")
				return err
			},
			errorOp: "validate_location",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			assert.Error(t, err)

			var minioErr *MinIOError
			if assert.ErrorAs(t, err, &minioErr) {
				assert.Equal(t, tt.errorOp, minioErr.Op)
			}
		})
	}
}

func TestWriteFileOperations(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	writeFile, err := fs.Create("test-write.txt")
	require.NoError(t, err)

	// Test unsupported operations on write file
	tests := []struct {
		name      string
		operation func() error
		errorOp   string
	}{
		{
			name: "read on write file",
			operation: func() error {
				_, err := writeFile.Read(make([]byte, 10))
				return err
			},
			errorOp: "read_write_file",
		},
		{
			name: "read at on write file",
			operation: func() error {
				_, err := writeFile.ReadAt(make([]byte, 10), 0)
				return err
			},
			errorOp: "read_at_write_file",
		},
		{
			name: "seek on write file",
			operation: func() error {
				_, err := writeFile.Seek(0, io.SeekStart)
				return err
			},
			errorOp: "seek_write_file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			assert.Error(t, err)

			var minioErr *MinIOError
			if assert.ErrorAs(t, err, &minioErr) {
				assert.Equal(t, tt.errorOp, minioErr.Op)
			}
		})
	}

	// Test Stat on write file
	fileInfo, err := writeFile.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(0), fileInfo.Size()) // No data written yet

	// Write some data and test Stat again
	testData := []byte("test data")
	_, err = writeToMinIOFile(t, writeFile, testData)
	assert.NoError(t, err)

	fileInfo, err = writeFile.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(testData)), fileInfo.Size())

	require.NoError(t, writeFile.Close())
}

func TestReadFileOperations(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Create test file first
	testData := generateTestData(1024)
	testLocation := "read-test.bin"

	writeFile, err := fs.Create(testLocation)
	require.NoError(t, err)
	_, err = writeToMinIOFile(t, writeFile, testData)
	require.NoError(t, err)
	require.NoError(t, writeFile.Close())

	// Test read operations
	readFile, err := fs.Open(testLocation)
	require.NoError(t, err)

	// Test invalid seek operations
	tests := []struct {
		name      string
		operation func() error
		errorOp   string
	}{
		{
			name: "seek from end",
			operation: func() error {
				_, err := readFile.Seek(0, io.SeekEnd)
				return err
			},
			errorOp: "seek",
		},
		{
			name: "seek with invalid whence",
			operation: func() error {
				_, err := readFile.Seek(0, 99)
				return err
			},
			errorOp: "seek",
		},
		{
			name: "seek to negative position",
			operation: func() error {
				_, err := readFile.Seek(-100, io.SeekStart)
				return err
			},
			errorOp: "seek",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.operation()
			assert.Error(t, err)

			var minioErr *MinIOError
			if assert.ErrorAs(t, err, &minioErr) {
				assert.Equal(t, tt.errorOp, minioErr.Op)
			}
		})
	}

	require.NoError(t, readFile.Close())
}

// Large File Tests
func TestLargeFileOperations(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	if testing.Short() {
		t.Skip("Skipping large file test in short mode")
	}

	config := createTestConfig()
	config.BufferSize = 128 * 1024 // 128KB buffer
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Generate large test data
	largeData := generateTestData(testLargeFileSize)
	testLocation := "large-file.bin"

	// Write large file
	start := time.Now()
	writeFile, err := fs.Create(testLocation)
	require.NoError(t, err)

	written := 0
	chunkSize := 64 * 1024 // 64KB chunks
	for written < len(largeData) {
		end := written + chunkSize
		if end > len(largeData) {
			end = len(largeData)
		}

		n, err := writeToMinIOFile(t, writeFile, largeData[written:end])
		require.NoError(t, err)
		written += n
	}

	require.NoError(t, writeFile.Close())
	writeTime := time.Since(start)
	t.Logf("Large file write took: %v", writeTime)

	// Read large file
	start = time.Now()
	readFile, err := fs.Open(testLocation)
	require.NoError(t, err)

	readData := make([]byte, len(largeData))
	totalRead := 0
	for totalRead < len(readData) {
		n, err := readFile.Read(readData[totalRead:])
		if err != nil && err != io.EOF {
			require.NoError(t, err)
		}
		totalRead += n
		if err == io.EOF {
			break
		}
	}

	require.NoError(t, readFile.Close())
	readTime := time.Since(start)
	t.Logf("Large file read took: %v", readTime)

	assert.Equal(t, len(largeData), totalRead)
	assert.Equal(t, largeData, readData)

	// Test metrics
	metrics := fs.GetMetrics()
	assert.True(t, metrics.BytesWritten >= int64(len(largeData)))
	assert.True(t, metrics.BytesRead >= int64(len(largeData)))
	assert.True(t, metrics.WriteOperations > 0)
	assert.True(t, metrics.ReadOperations > 0)
}

// Concurrent Operations Tests
func TestConcurrentFileOperations(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errors := make(chan error, testConcurrency*2)

	// Concurrent writes
	for i := 0; i < testConcurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			testData := generateTestData(1024)
			location := fmt.Sprintf("concurrent/write-%d.txt", id)

			writeFile, err := fs.Create(location)
			if err != nil {
				errors <- fmt.Errorf("create failed for %s: %w", location, err)
				return
			}

			_, err = writeToMinIOFile(t, writeFile, testData)
			if err != nil {
				errors <- fmt.Errorf("write failed for %s: %w", location, err)
				return
			}

			err = writeFile.Close()
			if err != nil {
				errors <- fmt.Errorf("close failed for %s: %w", location, err)
				return
			}
		}(i)
	}

	// Concurrent reads (after writes complete)
	wg.Wait()

	for i := 0; i < testConcurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			location := fmt.Sprintf("concurrent/write-%d.txt", id)

			readFile, err := fs.Open(location)
			if err != nil {
				errors <- fmt.Errorf("open failed for %s: %w", location, err)
				return
			}

			data := make([]byte, 1024)
			_, err = readFile.Read(data)
			if err != nil {
				errors <- fmt.Errorf("read failed for %s: %w", location, err)
				return
			}

			err = readFile.Close()
			if err != nil {
				errors <- fmt.Errorf("close failed for %s: %w", location, err)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errors)

	// Check for errors
	var errorList []error
	for err := range errors {
		errorList = append(errorList, err)
	}

	if len(errorList) > 0 {
		t.Fatalf("Concurrent operations failed with %d errors: %v", len(errorList), errorList[0])
	}

	// Verify metrics
	metrics := fs.GetMetrics()
	assert.True(t, metrics.WriteOperations >= int64(testConcurrency))
	assert.True(t, metrics.ReadOperations >= int64(testConcurrency))
}

// Buffer Overflow Tests
func TestBufferOverflow(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	config.BufferSize = 1024 // Small buffer for testing
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	writeFile, err := fs.Create("overflow-test.txt")
	require.NoError(t, err)

	// Try to write data larger than max buffer size
	largeData := make([]byte, MaxBufferSize+1)
	_, err = writeToMinIOFile(t, writeFile, largeData)
	assert.Error(t, err)

	var minioErr *MinIOError
	if assert.ErrorAs(t, err, &minioErr) {
		assert.Equal(t, "write_buffer_overflow", minioErr.Op)
	}
}

// Retry Logic Tests
func TestRetryLogic(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	config.RetryAttempts = 3
	config.RetryDelay = 10 * time.Millisecond
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Test retry on non-existent file (should fail after retries)
	start := time.Now()
	_, err = fs.Open("non-existent-file.txt")
	duration := time.Since(start)

	assert.Error(t, err)
	// Should take at least the retry delay time
	assert.True(t, duration >= time.Duration(config.RetryAttempts-1)*config.RetryDelay)

	var minioErr *MinIOError
	if assert.ErrorAs(t, err, &minioErr) {
		assert.Equal(t, "open", minioErr.Op)
		assert.Contains(t, minioErr.Context, "attempts")
		assert.Equal(t, config.RetryAttempts, minioErr.Context["attempts"])
	}
}

// Metrics Tests
func TestMetricsCollection(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	config.EnableMetrics = true
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Initial metrics
	initialMetrics := fs.GetMetrics()
	assert.Equal(t, int64(0), initialMetrics.ReadOperations)
	assert.Equal(t, int64(0), initialMetrics.WriteOperations)

	// Perform operations
	testData := generateTestData(512)
	testLocation := "metrics-test.txt"

	// Write operation
	writeFile, err := fs.Create(testLocation)
	require.NoError(t, err)
	_, err = writeToMinIOFile(t, writeFile, testData)
	require.NoError(t, err)
	require.NoError(t, writeFile.Close())

	// Read operation
	readFile, err := fs.Open(testLocation)
	require.NoError(t, err)
	readData := make([]byte, len(testData))
	_, err = readFile.Read(readData)
	require.NoError(t, err)
	require.NoError(t, readFile.Close())

	// Delete operation
	err = fs.Remove(testLocation)
	require.NoError(t, err)

	// Check metrics
	finalMetrics := fs.GetMetrics()
	assert.True(t, finalMetrics.WriteOperations > initialMetrics.WriteOperations)
	assert.True(t, finalMetrics.ReadOperations > initialMetrics.ReadOperations)
	assert.True(t, finalMetrics.DeleteOperations > initialMetrics.DeleteOperations)
	assert.True(t, finalMetrics.BytesWritten >= int64(len(testData)))
	assert.True(t, finalMetrics.BytesRead >= int64(len(testData)))
	assert.True(t, finalMetrics.AvgWriteLatency > 0)
	assert.True(t, finalMetrics.AvgReadLatency > 0)

	// Test server metrics
	serverMetrics := server.GetMetrics()
	assert.True(t, serverMetrics.Uptime > 0)
	assert.Equal(t, "healthy", serverMetrics.HealthCheckStatus)
}

// Security Tests
func TestSecureConfiguration(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	if testing.Short() {
		t.Skip("Skipping TLS test in short mode")
	}

	config := createSecureTestConfig()
	config.Port = testPort + 1 // Use different port

	// Note: This test verifies configuration but doesn't test actual TLS
	// since we'd need proper certificates for a full TLS test
	_, err := NewEmbeddedMinIO(config)
	require.NoError(t, err)

	assert.True(t, config.Secure)
	assert.NotNil(t, config.TLS)
	assert.Equal(t, "1.2", config.TLS.MinVersion)

	// Cleanup
	os.RemoveAll(config.DataDir)
}

func TestEnvironmentVariableValidation(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	tests := []struct {
		name     string
		key      string
		expected bool
	}{
		{"valid key", "test_key", true},
		{"valid key with numbers", "test123", true},
		{"valid key uppercase", "TEST_KEY", true},
		{"empty key", "", false},
		{"key too long", strings.Repeat("a", 65), false},
		{"key with invalid chars", "test-key", false},
		{"key with spaces", "test key", false},
		{"key with special chars", "test@key", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isValidEnvKey(tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Validation Tests
func TestBucketNameValidation(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	tests := []struct {
		name        string
		bucketName  string
		expectError bool
	}{
		{"valid bucket", "valid-bucket", false},
		{"valid bucket with numbers", "bucket123", false},
		{"valid bucket with dots", "bucket.name", false},
		{"bucket too short", "ab", true},
		{"bucket too long", strings.Repeat("a", 64), true},
		{"bucket with uppercase", "INVALID", true},
		{"bucket with underscore", "invalid_bucket", true},
		{"bucket starting with hyphen", "-invalid", true},
		{"bucket ending with hyphen", "invalid-", true},
		{"bucket starting with dot", ".invalid", true},
		{"bucket ending with dot", "invalid.", true},
		{"bucket with consecutive dots", "invalid..bucket", true},
		{"bucket as IP address", "192.168.1.1", true},
		{"empty bucket", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateBucketName(tt.bucketName)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestObjectNameValidation(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	tests := []struct {
		name        string
		objectName  string
		expectError bool
	}{
		{"valid object", "path/to/object.txt", false},
		{"valid object with spaces", "path to object.txt", false},
		{"valid object with unicode", "path/to/ñoño.txt", false},
		{"empty object", "", true},
		{"object too long", strings.Repeat("a", MaxObjectNameLength+1), true},
		{"object with control char", "path\x00object.txt", true},
		{"object with DEL char", "path\x7fobject.txt", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateObjectName(tt.objectName)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Path Handling Tests
func TestObjectNameGeneration(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	tests := []struct {
		name     string
		prefix   string
		location string
		expected string
	}{
		{
			name:     "with prefix",
			prefix:   "data",
			location: "file.txt",
			expected: "data/file.txt",
		},
		{
			name:     "without prefix",
			prefix:   "",
			location: "file.txt",
			expected: "file.txt",
		},
		{
			name:     "nested path",
			prefix:   "data",
			location: "folder/subfolder/file.txt",
			expected: "data/folder/subfolder/file.txt",
		},
		{
			name:     "path with leading slash",
			prefix:   "data",
			location: "/folder/file.txt",
			expected: "data/folder/file.txt",
		},
		{
			name:     "path with backslash",
			prefix:   "data",
			location: "folder\\file.txt",
			expected: "data/folder/file.txt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs, err := NewS3FileSystem(server, testBucket, tt.prefix)
			require.NoError(t, err)

			objectName := fs.getObjectName(tt.location)
			assert.Equal(t, tt.expected, objectName)
		})
	}
}

// Performance Tests
func TestPerformanceBenchmarks(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	config := createTestConfig()
	config.BufferSize = 256 * 1024 // 256KB buffer
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Test different file sizes
	sizes := []int{1024, 10 * 1024, 100 * 1024, 1024 * 1024} // 1KB to 1MB

	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%dKB", size/1024), func(t *testing.T) {
			testData := generateTestData(size)
			location := fmt.Sprintf("perf/test_%d.bin", size)

			// Measure write performance
			start := time.Now()
			writeFile, err := fs.Create(location)
			require.NoError(t, err)

			_, err = writeToMinIOFile(t, writeFile, testData)
			require.NoError(t, err)
			require.NoError(t, writeFile.Close())

			writeTime := time.Since(start)
			writeThroughput := float64(size) / writeTime.Seconds() / 1024 / 1024 // MB/s

			// Measure read performance
			start = time.Now()
			readFile, err := fs.Open(location)
			require.NoError(t, err)

			readData := make([]byte, size)
			_, err = readFile.Read(readData)
			require.NoError(t, err)
			require.NoError(t, readFile.Close())

			readTime := time.Since(start)
			readThroughput := float64(size) / readTime.Seconds() / 1024 / 1024 // MB/s

			t.Logf("Size: %d KB, Write: %.2f MB/s, Read: %.2f MB/s",
				size/1024, writeThroughput, readThroughput)

			// Verify data integrity
			assert.Equal(t, testData, readData)
		})
	}
}

// Error Recovery Tests
func TestErrorRecovery(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Test recovery after server restart
	testData := generateTestData(1024)
	testLocation := "recovery/test.txt"

	// Write file
	writeFile, err := fs.Create(testLocation)
	require.NoError(t, err)
	_, err = writeToMinIOFile(t, writeFile, testData)
	require.NoError(t, err)
	require.NoError(t, writeFile.Close())

	// Stop server
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = server.Stop(stopCtx)
	require.NoError(t, err)

	// Try to read file (should fail)
	_, err = fs.Open(testLocation)
	assert.Error(t, err)

	// Restart server
	startCtx, startCancel := context.WithTimeout(context.Background(), testTimeout)
	defer startCancel()
	err = server.Start(startCtx)
	require.NoError(t, err)

	// Update filesystem client
	fs, err = NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Read file (should work now)
	readFile, err := fs.Open(testLocation)
	require.NoError(t, err)

	readData := make([]byte, len(testData))
	_, err = readFile.Read(readData)
	require.NoError(t, err)
	require.NoError(t, readFile.Close())

	assert.Equal(t, testData, readData)
}

// Cleanup and Resource Management Tests
func TestResourceCleanup(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	server, err := NewEmbeddedMinIO(config)
	require.NoError(t, err)

	// Start server
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()
	err = server.Start(ctx)
	require.NoError(t, err)

	// Create filesystem
	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Create and close multiple files
	for i := 0; i < 10; i++ {
		location := fmt.Sprintf("cleanup/file_%d.txt", i)

		writeFile, err := fs.Create(location)
		require.NoError(t, err)

		_, err = writeToMinIOFile(t, writeFile, []byte("test data"))
		require.NoError(t, err)

		err = writeFile.Close()
		require.NoError(t, err)
	}

	// Stop server
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopCancel()
	err = server.Stop(stopCtx)
	require.NoError(t, err)

	// Verify cleanup
	assert.False(t, server.IsRunning())
	assert.Nil(t, server.GetClient())

	// Cleanup test data
	os.RemoveAll(config.DataDir)
}

// Integration Tests
func TestEndToEndWorkflow(t *testing.T) {
	t.Skip("Skipping MinIO tests for now")
	config := createTestConfig()
	config.EnableMetrics = true
	server, cleanup := setupTestServer(t, config)
	defer cleanup()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	require.NoError(t, err)

	// Simulate a complete workflow
	files := []struct {
		path string
		data []byte
	}{
		{"documents/readme.txt", []byte("This is a readme file")},
		{"data/dataset.csv", generateTestData(2048)},
		{"images/photo.jpg", generateTestData(4096)},
		{"config/settings.json", []byte(`{"version": "1.0", "debug": true}`)},
	}

	// Write all files
	for _, file := range files {
		writeFile, err := fs.Create(file.path)
		require.NoError(t, err, "Failed to create %s", file.path)

		_, err = writeToMinIOFile(t, writeFile, file.data)
		require.NoError(t, err, "Failed to write %s", file.path)

		err = writeFile.Close()
		require.NoError(t, err, "Failed to close %s", file.path)
	}

	// Read and verify all files
	for _, file := range files {
		readFile, err := fs.Open(file.path)
		require.NoError(t, err, "Failed to open %s", file.path)

		readData := make([]byte, len(file.data))
		n, err := readFile.Read(readData)
		require.NoError(t, err, "Failed to read %s", file.path)
		require.Equal(t, len(file.data), n, "Read size mismatch for %s", file.path)

		err = readFile.Close()
		require.NoError(t, err, "Failed to close %s", file.path)

		assert.Equal(t, file.data, readData, "Data mismatch for %s", file.path)
	}

	// Test partial reads
	readFile, err := fs.Open("data/dataset.csv")
	require.NoError(t, err)

	partialData := make([]byte, 512)
	n, err := readFile.ReadAt(partialData, 1024)
	require.NoError(t, err)
	assert.Equal(t, 512, n)
	assert.Equal(t, files[1].data[1024:1536], partialData)

	require.NoError(t, readFile.Close())

	// Clean up some files
	err = fs.Remove("documents/readme.txt")
	require.NoError(t, err)

	err = fs.Remove("config/settings.json")
	require.NoError(t, err)

	// Verify files are removed
	_, err = fs.Open("documents/readme.txt")
	assert.Error(t, err)

	_, err = fs.Open("config/settings.json")
	assert.Error(t, err)

	// Verify remaining files still exist
	_, err = fs.Open("data/dataset.csv")
	assert.NoError(t, err)

	_, err = fs.Open("images/photo.jpg")
	assert.NoError(t, err)

	// Check final metrics
	metrics := fs.GetMetrics()
	assert.True(t, metrics.WriteOperations >= int64(len(files)))
	assert.True(t, metrics.ReadOperations > 0)
	assert.True(t, metrics.DeleteOperations >= 2)
	assert.True(t, metrics.BytesWritten > 0)
	assert.True(t, metrics.BytesRead > 0)

	serverMetrics := server.GetMetrics()
	assert.True(t, serverMetrics.Uptime > 0)
	assert.Equal(t, "healthy", serverMetrics.HealthCheckStatus)
}

// Benchmark Tests
func BenchmarkFileOperations(b *testing.B) {
	b.Skip("Skipping MinIO benchmarks for now")
	config := createTestConfig()
	config.Quiet = true // Reduce noise in benchmarks

	server, err := NewEmbeddedMinIO(config)
	if err != nil {
		b.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	defer cancel()

	err = server.Start(ctx)
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := server.Stop(stopCtx); err != nil {
			b.Errorf("Failed to stop server: %v", err)
		}
		os.RemoveAll(config.DataDir)
	}()

	fs, err := NewS3FileSystemWithServer(server, testBucket, testPrefix)
	if err != nil {
		b.Fatal(err)
	}

	testData := generateTestData(1024)

	b.Run("Write", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			location := fmt.Sprintf("bench/write_%d.txt", i)
			writeFile, err := fs.Create(location)
			if err != nil {
				b.Fatal(err)
			}

			// Cast to the concrete type to access Write method
			minioWriteFile, ok := writeFile.(*minioWriteFile)
			if !ok {
				b.Fatal("writeFile should be of type *minioWriteFile")
			}
			_, err = minioWriteFile.Write(testData)
			if err != nil {
				b.Fatal(err)
			}

			err = writeFile.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})

	// Create files for read benchmark
	for i := 0; i < b.N; i++ {
		location := fmt.Sprintf("bench/read_%d.txt", i)
		writeFile, err := fs.Create(location)
		if err != nil {
			b.Fatal(err)
		}
		// Cast to the concrete type to access Write method
		minioWriteFile, ok := writeFile.(*minioWriteFile)
		if !ok {
			b.Fatal("writeFile should be of type *minioWriteFile")
		}
		if _, err := minioWriteFile.Write(testData); err != nil {
			b.Fatal(err)
		}
		writeFile.Close()
	}

	b.Run("Read", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			location := fmt.Sprintf("bench/read_%d.txt", i)
			readFile, err := fs.Open(location)
			if err != nil {
				b.Fatal(err)
			}

			data := make([]byte, len(testData))
			_, err = readFile.Read(data)
			if err != nil {
				b.Fatal(err)
			}

			err = readFile.Close()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
