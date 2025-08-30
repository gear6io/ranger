package integration_tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTestServer(t *testing.T) {
	TestWithServer(t, func(t *testing.T, server *TestServer) {
		// Test that server is running
		assert.NotNil(t, server.server)

		// Test that we can get a client
		client := server.GetClient(t)
		require.NotNil(t, client)
		defer client.Close()

		// Test basic connection with timeout
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := client.Ping(ctx)
		assert.NoError(t, err)
	})
}

func TestTestServerMultipleRuns(t *testing.T) {
	// Test that multiple test runs work correctly
	for i := 0; i < 2; i++ {
		t.Run(fmt.Sprintf("Run_%d", i), func(t *testing.T) {
			TestWithServer(t, func(t *testing.T, server *TestServer) {
				client := server.GetClient(t)
				require.NotNil(t, client)
				defer client.Close()

				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				err := client.Ping(ctx)
				assert.NoError(t, err)
			})
		})
	}
}
