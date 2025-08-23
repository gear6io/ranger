package registry

import (
	"context"
	"log"

	"github.com/TFMV/icebox/pkg/errors"
	"github.com/uptrace/bun"
)

// Package-specific error codes for hybrid deployment
var (
	RegistryDeploymentFailed      = errors.MustNewCode("registry.deployment_failed")
	RegistryMigrationStatusFailed = errors.MustNewCode("registry.migration_status_failed")
)

// DeploymentStatus represents the deployment status
type DeploymentStatus struct {
	Status         string            `json:"status"`
	CurrentVersion int               `json:"current_version"`
	LatestVersion  int               `json:"latest_version"`
	PendingCount   int               `json:"pending_count"`
	AppliedCount   int               `json:"applied_count"`
	SchemaValid    bool              `json:"schema_valid"`
	Migrations     []MigrationStatus `json:"migrations"`
}

// HybridDeploymentManager combines bun migrations with your deployment safety
type HybridDeploymentManager struct {
	bunMigrator *BunMigrationManager
	store       *Store
}

// NewHybridDeploymentManager creates a new hybrid deployment manager
func NewHybridDeploymentManager(store *Store, bunMigrator *BunMigrationManager) *HybridDeploymentManager {
	return &HybridDeploymentManager{
		bunMigrator: bunMigrator,
		store:       store,
	}
}

// EnsureDeploymentReady ensures the database is ready for deployment
// This method will fail fast if migrations fail, causing deployment to fail
func (hdm *HybridDeploymentManager) EnsureDeploymentReady(ctx context.Context) error {
	log.Println("🔍 Checking database deployment readiness with hybrid system...")

	// Step 1: Run bun migrations
	log.Println("🔄 Running bun migrations...")
	if err := hdm.bunMigrator.MigrateToLatest(ctx); err != nil {
		return errors.New(RegistryDeploymentFailed, "deployment failed - bun migrations failed").WithCause(err)
	}
	log.Println("✅ Bun migrations completed successfully")

	// Step 2: Verify bun schema
	log.Println("🔍 Verifying bun schema...")
	if err := hdm.bunMigrator.VerifySchema(ctx); err != nil {
		return errors.New(RegistryDeploymentFailed, "deployment failed - bun schema verification failed").WithCause(err)
	}
	log.Println("✅ Bun schema verification passed")

	// Step 3: Your existing deployment logic (if needed)
	// This gives you the flexibility to add custom deployment checks
	if err := hdm.runCustomDeploymentChecks(ctx); err != nil {
		return errors.New(RegistryDeploymentFailed, "deployment failed - custom checks failed").WithCause(err)
	}

	log.Println("🚀 Database is ready for deployment")
	return nil
}

// runCustomDeploymentChecks runs any custom deployment logic you want
func (hdm *HybridDeploymentManager) runCustomDeploymentChecks(ctx context.Context) error {
	// Add your custom deployment logic here
	// For example:
	// - Check file permissions
	// - Verify external dependencies
	// - Run health checks
	// - Validate configuration

	log.Println("✅ Custom deployment checks passed")
	return nil
}

// GetDeploymentStatus returns detailed deployment status
func (hdm *HybridDeploymentManager) GetDeploymentStatus(ctx context.Context) (*DeploymentStatus, error) {
	// Get bun migration status
	bunStatus, err := hdm.bunMigrator.GetMigrationStatus(ctx)
	if err != nil {
		return nil, errors.New(RegistryMigrationStatusFailed, "failed to get bun migration status").WithCause(err)
	}

	// Get current version
	currentVersion, err := hdm.bunMigrator.GetCurrentVersion(ctx)
	if err != nil {
		return nil, errors.New(RegistryMigrationStatusFailed, "failed to get current version").WithCause(err)
	}

	// Count pending and applied migrations
	var pendingCount, appliedCount int
	for _, migration := range bunStatus {
		if migration.Status == "pending" {
			pendingCount++
		} else {
			appliedCount++
		}
	}

	// Determine overall status
	status := "ready"
	if pendingCount > 0 {
		status = "pending"
	}

	// Check if schema verification would pass
	schemaValid := true
	if err := hdm.bunMigrator.VerifySchema(ctx); err != nil {
		schemaValid = false
	}

	return &DeploymentStatus{
		Status:         status,
		CurrentVersion: currentVersion,
		LatestVersion:  1, // For now, hardcoded
		PendingCount:   pendingCount,
		AppliedCount:   appliedCount,
		SchemaValid:    schemaValid,
		Migrations:     bunStatus,
	}, nil
}

// GetBunDB returns the underlying bun DB for advanced operations
func (hdm *HybridDeploymentManager) GetBunDB() *bun.DB {
	return hdm.bunMigrator.GetDB()
}

// GetStore returns the underlying store for your existing operations
func (hdm *HybridDeploymentManager) GetStore() *Store {
	return hdm.store
}
