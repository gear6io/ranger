package sdk

import (
	"context"
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/gear6io/ranger/server"
	"github.com/gear6io/ranger/server/config"
)

// TestServer provides a real server for testing
type TestServer struct {
	server  *server.Server
	tempDir string
}

// NewTestServer creates a new test server
func NewTestServer(t *testing.T) *TestServer {
	t.Helper()

	// Create temporary directory for test data
	tempDir, err := os.MkdirTemp("", "ranger-test-*")
	require.NoError(t, err)

	// Create minimal test configuration
	cfg := &config.Config{
		Storage: config.StorageConfig{
			DataPath: tempDir,
			Catalog: config.CatalogConfig{
				Type: "json",
			},
		},
	}

	// Create logger (minimal output for tests)
	logger := zerolog.New(os.Stdout).Level(zerolog.ErrorLevel)

	// Create server
	srv, err := server.New(cfg, logger)
	require.NoError(t, err)

	return &TestServer{
		server:  srv,
		tempDir: tempDir,
	}
}

// Start starts the test server
func (s *TestServer) Start() error {
	ctx := context.Background()
	if err := s.server.Start(ctx); err != nil {
		return err
	}

	// Wait for native server to be ready
	return s.WaitForReady()
}

// Stop stops the test server
func (s *TestServer) Stop() error {
	if s.server != nil {
		return s.server.Shutdown(context.Background())
	}
	return nil
}

// Cleanup cleans up test resources
func (s *TestServer) Cleanup() error {
	// Stop server
	if err := s.Stop(); err != nil {
		return err
	}

	// Remove temp directory
	if s.tempDir != "" {
		return os.RemoveAll(s.tempDir)
	}

	return nil
}

// GetClient creates a new SDK client connected to the test server
func (s *TestServer) GetClient(t *testing.T) *Client {
	t.Helper()

	// Use the default native server port
	client, err := Open(&Options{
		Addr: []string{"127.0.0.1:2849"},
		Auth: Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debug: false,
		Settings: Settings{
			"max_execution_time": 60,
		},
	})
	require.NoError(t, err)

	return client
}

// WaitForReady waits for the server to be ready to accept connections
func (s *TestServer) WaitForReady() error {
	timeout := 10 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// Try to connect to the server
		conn, err := net.DialTimeout("tcp", "127.0.0.1:2849", 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return fmt.Errorf("server not ready after %v", timeout)
}

// TestWithServer runs a test with a real server instance
func TestWithServer(t *testing.T, testFunc func(*testing.T, *TestServer)) {
	t.Helper()

	server := NewTestServer(t)
	defer server.Cleanup()

	// Start server
	err := server.Start()
	require.NoError(t, err)

	// Wait for server to be ready
	err = server.WaitForReady()
	require.NoError(t, err)

	// Run test with timeout
	done := make(chan bool)
	testErr := make(chan error, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				testErr <- fmt.Errorf("test panicked: %v", r)
			}
		}()

		testFunc(t, server)
		done <- true
	}()

	select {
	case <-done:
		// Test completed successfully
		select {
		case err := <-testErr:
			t.Fatalf("Test failed with panic: %v", err)
		default:
			// No panic, test completed normally
		}
	case err := <-testErr:
		t.Fatalf("Test panicked: %v", err)
	case <-time.After(20 * time.Second): // Reduced from 25s to 20s to match overall test timeout
		t.Fatal("Test timed out after 20 seconds")
	}
}
