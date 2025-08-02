package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/json"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/display"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var initCmd = &cobra.Command{
	Use:   "init [directory]",
	Short: "Initialize a new Icebox project",
	Long: `Initialize a new Icebox project with a catalog and configuration.

This command creates a new directory (default: icebox-lakehouse) and sets up:
- .icebox.yml configuration file
- Catalog (SQLite database or JSON file)
- Local filesystem storage directory
- .icebox/display.yaml display configuration

If no directory is specified, it creates "icebox-lakehouse" in the current location.`,
	Args: cobra.MaximumNArgs(1),
	RunE: runInit,
}

type initOptions struct {
	catalog string
	storage string
}

var initOpts = &initOptions{}

func init() {
	rootCmd.AddCommand(initCmd)

	initCmd.Flags().StringVar(&initOpts.catalog, "catalog", "sqlite", "catalog type (sqlite|rest|json)")
	initCmd.Flags().StringVar(&initOpts.storage, "storage", "fs", "storage type (fs|s3|mem)")
}

func runInit(cmd *cobra.Command, args []string) error {
	// Handle nil command or nil context (for testing)
	var ctx context.Context
	if cmd != nil {
		ctx = cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}
	} else {
		ctx = context.Background()
	}

	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	// Determine target directory
	var targetDir string
	if len(args) > 0 {
		targetDir = args[0]
	} else {
		targetDir = "icebox-lakehouse"
	}

	if logger != nil {
		logger.Info().Str("cmd", "init").Str("target_dir", targetDir).Msg("Starting project initialization")
	}

	// Get absolute path
	absPath, err := filepath.Abs(targetDir)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "init").Str("target_dir", targetDir).Err(err).Msg("Failed to get absolute path")
		}
		d.Error("Failed to get absolute path: %v", err)
		return err
	}

	// Check if directory exists, create if it doesn't
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		if err := os.MkdirAll(absPath, 0755); err != nil {
			if logger != nil {
				logger.Error().Str("cmd", "init").Str("path", absPath).Err(err).Msg("Failed to create directory")
			}
			d.Error("Failed to create directory %s: %v", absPath, err)
			return err
		}
		if logger != nil {
			logger.Info().Str("cmd", "init").Str("path", absPath).Msg("Created directory")
		}
		d.Info("Created directory: %s", absPath)
	}

	// Check if already initialized
	configPath := filepath.Join(absPath, ".icebox.yml")
	if _, err := os.Stat(configPath); err == nil {
		if logger != nil {
			logger.Warn().Str("cmd", "init").Str("path", absPath).Msg("Directory already contains an Icebox project")
		}
		d.Error("Directory already contains an Icebox project (found .icebox.yml)")
		return fmt.Errorf("directory already contains an Icebox project (found .icebox.yml)")
	}

	// Create configuration
	cfg := &config.Config{
		Name: filepath.Base(absPath),
		Catalog: config.CatalogConfig{
			Type: initOpts.catalog,
		},
		Storage: config.StorageConfig{
			Type: initOpts.storage,
		},
	}

	// Initialize based on catalog type
	switch initOpts.catalog {
	case "sqlite":
		if err := initSQLiteCatalog(absPath, cfg); err != nil {
			if logger != nil {
				logger.Error().Str("cmd", "init").Err(err).Msg("Failed to initialize SQLite catalog")
			}
			d.Error("Failed to initialize SQLite catalog: %v", err)
			return err
		}
	case "json":
		if err := initJSONCatalog(absPath, cfg); err != nil {
			if logger != nil {
				logger.Error().Str("cmd", "init").Err(err).Msg("Failed to initialize JSON catalog")
			}
			d.Error("Failed to initialize JSON catalog: %v", err)
			return err
		}
	case "rest":
		d.Error("REST catalog initialization not yet implemented")
		return fmt.Errorf("REST catalog initialization not yet implemented")
	default:
		d.Error("Unsupported catalog type: %s", initOpts.catalog)
		return fmt.Errorf("unsupported catalog type: %s", initOpts.catalog)
	}

	// Initialize storage
	if err := initStorage(absPath, cfg); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "init").Err(err).Msg("Failed to initialize storage")
		}
		d.Error("Failed to initialize storage: %v", err)
		return err
	}

	// Write configuration file
	if err := config.WriteConfig(configPath, cfg); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "init").Err(err).Msg("Failed to write configuration")
		}
		d.Error("Failed to write configuration: %v", err)
		return err
	}

	// Actually initialize the catalog
	if err := initializeCatalog(cfg); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "init").Err(err).Msg("Failed to initialize catalog")
		}
		d.Error("Failed to initialize catalog: %v", err)
		return err
	}

	// Create default namespace for better user experience
	if err := createDefaultNamespace(cfg, d, logger); err != nil {
		// Non-fatal error - just warn the user
		if logger != nil {
			logger.Warn().Str("cmd", "init").Str("path", absPath).Err(err).Msg("Could not create default namespace")
		}
		d.Warning("Could not create default namespace: %v", err)
	}

	// Initialize display configuration
	if err := initDisplayConfig(absPath); err != nil {
		// Non-fatal error - just warn the user
		if logger != nil {
			logger.Warn().Str("cmd", "init").Str("path", absPath).Err(err).Msg("Could not create display configuration")
		}
		d.Warning("Could not create display configuration: %v", err)
	}

	// Create log file in the project directory
	if err := initLogFile(absPath); err != nil {
		// Non-fatal error - just warn the user
		if logger != nil {
			logger.Warn().Str("cmd", "init").Str("path", absPath).Err(err).Msg("Could not create log file")
		}
		d.Warning("Could not create log file: %v", err)
	}

	if logger != nil {
		logger.Info().Str("cmd", "init").Str("path", absPath).Str("catalog", cfg.Catalog.Type).Str("storage", cfg.Storage.Type).Msg("Successfully initialized Icebox project")
	}

	d.Success("Initialized Icebox project in %s", absPath)
	d.Info("   Catalog: %s", cfg.Catalog.Type)
	d.Info("   Storage: %s", cfg.Storage.Type)
	d.Info("")
	d.Info("Next steps:")
	d.Info("   icebox import your-data.parquet --table your_table")
	d.Info("   icebox sql 'SELECT * FROM your_table LIMIT 10'")

	return nil
}

func initSQLiteCatalog(projectDir string, cfg *config.Config) error {
	// Create catalog directory
	catalogDir := filepath.Join(projectDir, ".icebox", "catalog")
	if err := os.MkdirAll(catalogDir, 0755); err != nil {
		return fmt.Errorf("failed to create catalog directory: %w", err)
	}

	// Set catalog database path
	dbPath := filepath.Join(catalogDir, "catalog.db")
	cfg.Catalog.SQLite = &config.SQLiteConfig{
		Path: dbPath,
	}

	return nil
}

func initJSONCatalog(projectDir string, cfg *config.Config) error {
	// Create catalog directory
	catalogDir := filepath.Join(projectDir, ".icebox", "catalog")
	if err := os.MkdirAll(catalogDir, 0755); err != nil {
		return fmt.Errorf("failed to create catalog directory: %w", err)
	}

	// Create warehouse directory
	warehouseDir := filepath.Join(projectDir, ".icebox", "data")
	if err := os.MkdirAll(warehouseDir, 0755); err != nil {
		return fmt.Errorf("failed to create warehouse directory: %w", err)
	}

	// Set catalog JSON file path and warehouse
	catalogPath := filepath.Join(catalogDir, "catalog.json")
	cfg.Catalog.JSON = &config.JSONConfig{
		URI:       catalogPath,
		Warehouse: warehouseDir,
	}

	return nil
}

func initStorage(projectDir string, cfg *config.Config) error {
	switch cfg.Storage.Type {
	case "fs":
		// Create data directory
		dataDir := filepath.Join(projectDir, ".icebox", "data")
		if err := os.MkdirAll(dataDir, 0755); err != nil {
			return fmt.Errorf("failed to create data directory: %w", err)
		}
		cfg.Storage.FileSystem = &config.FileSystemConfig{
			RootPath: dataDir,
		}
	case "mem":
		// Memory storage needs no initialization
		cfg.Storage.Memory = &config.MemoryConfig{}
	case "s3":
		return fmt.Errorf("S3 storage initialization not yet implemented")
	default:
		return fmt.Errorf("unsupported storage type: %s", cfg.Storage.Type)
	}

	return nil
}

// initializeCatalog creates and initializes the actual catalog
func initializeCatalog(cfg *config.Config) error {
	switch cfg.Catalog.Type {
	case "sqlite":
		// Create SQLite catalog to initialize the database
		catalog, err := sqlite.NewCatalog(cfg)
		if err != nil {
			return fmt.Errorf("failed to create SQLite catalog: %w", err)
		}
		defer catalog.Close()

		// The catalog is automatically initialized when created
		return nil
	case "json":
		// Create JSON catalog to initialize the catalog file
		catalog, err := json.NewCatalog(cfg)
		if err != nil {
			return fmt.Errorf("failed to create JSON catalog: %w", err)
		}
		defer catalog.Close()

		// The catalog is automatically initialized when created
		return nil
	default:
		return fmt.Errorf("unsupported catalog type: %s", cfg.Catalog.Type)
	}
}

// initDisplayConfig creates the default display configuration
func initDisplayConfig(projectDir string) error {
	// Create .icebox directory if it doesn't exist
	iceboxDir := filepath.Join(projectDir, ".icebox")
	if err := os.MkdirAll(iceboxDir, 0755); err != nil {
		return fmt.Errorf("failed to create .icebox directory: %w", err)
	}

	// Create default display configuration
	displayConfig := display.DefaultConfig()

	// Customize defaults for new projects
	displayConfig.Table.Pagination = 100      // Show more rows by default
	displayConfig.Table.UnicodeBorders = true // Enable Unicode borders
	displayConfig.Colors.Enabled = "auto"     // Auto-detect color support
	displayConfig.Timing = true               // Show query timing by default

	// Save display configuration
	configPath := filepath.Join(iceboxDir, "display.yaml")
	if err := display.SaveConfigToFile(displayConfig, configPath); err != nil {
		return fmt.Errorf("failed to save display configuration: %w", err)
	}

	return nil
}

// initLogFile creates an initial log file in the project directory
func initLogFile(projectDir string) error {
	logPath := filepath.Join(projectDir, "icebox.log")

	// Create an empty log file if it doesn't exist
	if _, err := os.Stat(logPath); os.IsNotExist(err) {
		file, err := os.Create(logPath)
		if err != nil {
			return fmt.Errorf("failed to create log file: %w", err)
		}
		defer file.Close()

		// Write initial log entry
		_, err = file.WriteString(fmt.Sprintf(`{"level":"info","app":"icebox","cmd":"init","time":%d,"message":"Icebox project initialized"}%s`,
			time.Now().Unix(), "\n"))
		if err != nil {
			return fmt.Errorf("failed to write initial log entry: %w", err)
		}
	}

	return nil
}

// createDefaultNamespace creates a default namespace for better user experience
func createDefaultNamespace(cfg *config.Config, d display.Display, logger *zerolog.Logger) error {
	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("failed to create catalog: %w", err)
	}
	defer cat.Close()

	ctx := context.Background()
	defaultNamespace := table.Identifier{"default"}

	// Check if default namespace already exists
	exists, err := cat.CheckNamespaceExists(ctx, defaultNamespace)
	if err != nil {
		return fmt.Errorf("failed to check namespace existence: %w", err)
	}

	if exists {
		if logger != nil {
			logger.Info().Str("cmd", "init").Msg("Default namespace already exists")
		}
		return nil
	}

	// Create the default namespace
	if err := cat.CreateNamespace(ctx, defaultNamespace, iceberg.Properties{}); err != nil {
		return fmt.Errorf("failed to create default namespace: %w", err)
	}

	if logger != nil {
		logger.Info().Str("cmd", "init").Msg("Created default namespace")
	}
	d.Info("Created default namespace")

	return nil
}
