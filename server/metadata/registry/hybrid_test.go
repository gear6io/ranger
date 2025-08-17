package registry

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestBunSystem(t *testing.T) {
	// Create temporary directory
	tempDir, err := os.MkdirTemp("", "bun_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "test.db")
	basePath := filepath.Join(tempDir, "data")

	// Test 1: Create store with bun migrations
	t.Run("CreateStoreWithBun", func(t *testing.T) {
		store, err := NewStore(dbPath, basePath)
		if err != nil {
			t.Fatalf("Failed to create store with bun: %v", err)
		}
		defer store.Close()

		if !store.IsUsingBun() {
			t.Error("Store should be using bun migrations")
		}

		if store.GetBunMigrationManager() == nil {
			t.Error("Bun migration manager should not be nil")
		}
	})

	// Test 2: Test hybrid deployment manager
	t.Run("HybridDeploymentManager", func(t *testing.T) {
		store, err := NewStore(dbPath, basePath)
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}
		defer store.Close()

		hybridManager := NewHybridDeploymentManager(store, store.GetBunMigrationManager())

		// Test deployment readiness
		ctx := context.Background()
		if err := hybridManager.EnsureDeploymentReady(ctx); err != nil {
			t.Fatalf("Deployment readiness check failed: %v", err)
		}

		// Test deployment status
		status, err := hybridManager.GetDeploymentStatus(ctx)
		if err != nil {
			t.Fatalf("Failed to get deployment status: %v", err)
		}

		if status.Status != "ready" {
			t.Errorf("Expected status 'ready', got '%s'", status.Status)
		}

		if !status.SchemaValid {
			t.Error("Schema should be valid")
		}
	})

	// Test 3: Test basic operations with bun
	t.Run("BasicOperationsWithBun", func(t *testing.T) {
		store, err := NewStore(dbPath, basePath)
		if err != nil {
			t.Fatalf("Failed to create store: %v", err)
		}
		defer store.Close()

		ctx := context.Background()

		// Create database
		if err := store.CreateDatabase(ctx, "testdb"); err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}

		// Check if database exists
		if !store.DatabaseExists(ctx, "testdb") {
			t.Error("Database should exist")
		}

		// Create table
		if _, err := store.CreateTable(ctx, "testdb", "testtable", []byte("{}"), "MEMORY", nil); err != nil {
			t.Fatalf("Failed to create table: %v", err)
		}

		// Check if table exists
		if !store.TableExists(ctx, "testdb", "testtable") {
			t.Error("Table should exist")
		}

		// List tables
		tables, err := store.ListTables(ctx, "testdb")
		if err != nil {
			t.Fatalf("Failed to list tables: %v", err)
		}

		if len(tables) != 1 || tables[0] != "testtable" {
			t.Errorf("Expected 1 table 'testtable', got %v", tables)
		}

		// List databases
		databases, err := store.ListDatabases(ctx)
		if err != nil {
			t.Fatalf("Failed to list databases: %v", err)
		}

		if len(databases) != 1 || databases[0] != "testdb" {
			t.Errorf("Expected 1 database 'testdb', got %v", databases)
		}
	})
}
