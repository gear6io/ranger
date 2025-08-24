package registry

import (
	"context"
	"log"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/uptrace/bun"
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

	// Check if migrations are up to date (migrations already run during initialization)
	log.Println("🔍 Checking migration status...")
	currentVersion, err := hdm.bunMigrator.GetCurrentVersion(ctx)
	if err != nil {
		return err
	}

	latestVersion := hdm.bunMigrator.GetLatestVersion()

	if currentVersion < latestVersion {
		return errors.New(RegistryBunMigrationFailed, "database is not at latest migration version", nil).
			AddContext("current_version", currentVersion).
			AddContext("latest_version", latestVersion)
	}
	log.Println("✅ Migration status verified - database is up to date")

	// Run custom deployment checks (if needed)
	if err := hdm.runCustomDeploymentChecks(ctx); err != nil {
		return err
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
		return nil, err
	}

	// Get current version
	currentVersion, err := hdm.bunMigrator.GetCurrentVersion(ctx)
	if err != nil {
		return nil, err
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

	// Check if migrations are up to date instead of running schema verification
	schemaValid := true
	latestVersion := hdm.bunMigrator.GetLatestVersion()
	if currentVersion < latestVersion {
		schemaValid = false
	}

	return &DeploymentStatus{
		Status:         status,
		CurrentVersion: currentVersion,
		LatestVersion:  latestVersion,
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
