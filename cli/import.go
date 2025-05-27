package cli

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/display"
	"github.com/TFMV/icebox/importer"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
)

var importCmd = &cobra.Command{
	Use:   "import [file]",
	Short: "Import a data file (Parquet or Avro) into an Iceberg table",
	Long: `Import a data file into an Iceberg table with automatic schema inference.

Supported formats:
- Parquet (.parquet)
- Avro (.avro)

This command will:
- Detect the file format automatically
- Read the file and infer the schema
- Create a namespace if it doesn't exist
- Create an Iceberg table with the inferred schema
- Copy the data to the table location

Examples:
  icebox import data.parquet --table my_table
  icebox import data.avro --table namespace.table_name
  icebox import data.parquet --table sales --namespace analytics
  icebox import data.avro --dry-run --infer-schema`,
	Args: cobra.ExactArgs(1),
	RunE: runImport,
}

type importOptions struct {
	tableName   string
	namespace   string
	inferSchema bool
	dryRun      bool
	overwrite   bool
	partitionBy []string
}

var importOpts = &importOptions{}

func init() {
	rootCmd.AddCommand(importCmd)

	importCmd.Flags().StringVar(&importOpts.tableName, "table", "", "target table name (namespace.table or just table)")
	if err := importCmd.MarkFlagRequired("table"); err != nil {
		panic(fmt.Sprintf("Failed to mark table flag as required: %v", err))
	}
	importCmd.Flags().StringVar(&importOpts.namespace, "namespace", "", "target namespace (optional, can be included in table name)")
	importCmd.Flags().BoolVar(&importOpts.inferSchema, "infer-schema", true, "automatically infer schema from data")
	importCmd.Flags().BoolVar(&importOpts.dryRun, "dry-run", false, "show what would be done without executing")
	importCmd.Flags().BoolVar(&importOpts.overwrite, "overwrite", false, "overwrite existing table")
	importCmd.Flags().StringSliceVar(&importOpts.partitionBy, "partition-by", nil, "partition columns (comma-separated)")
}

func runImport(cmd *cobra.Command, args []string) error {
	dataFile := args[0]

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

	if logger != nil {
		logger.Info().Str("cmd", "import").Str("file", dataFile).Str("table", importOpts.tableName).Msg("Starting import operation")
	}

	// Validate that the data file exists
	if _, err := os.Stat(dataFile); os.IsNotExist(err) {
		if logger != nil {
			logger.Error().Str("cmd", "import").Str("file", dataFile).Err(err).Msg("Data file does not exist")
		}
		d.Error("Data file does not exist: %s", dataFile)
		return fmt.Errorf("data file does not exist: %s", dataFile)
	}

	// Get absolute path to the data file
	absDataFile, err := filepath.Abs(dataFile)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "import").Str("file", dataFile).Err(err).Msg("Failed to get absolute path")
		}
		d.Error("Failed to get absolute path: %v", err)
		return err
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "import").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	if cmd != nil && cmd.Flag("verbose").Value.String() == "true" {
		d.Info("Using configuration: %s", configPath)
	}

	// Parse table identifier
	tableIdent, namespaceIdent, err := parseTableIdentifier(importOpts.tableName, importOpts.namespace)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "import").Str("table", importOpts.tableName).Err(err).Msg("Failed to parse table identifier")
		}
		d.Error("Failed to parse table identifier: %v", err)
		return err
	}

	// Create importer factory and detect file type
	factory := importer.NewImporterFactory(cfg)
	imp, importerType, err := factory.CreateImporter(absDataFile)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "import").Str("file", absDataFile).Err(err).Msg("Failed to create importer")
		}
		d.Error("Failed to create importer: %v", err)
		return err
	}
	defer imp.Close()

	if logger != nil {
		logger.Info().Str("cmd", "import").Str("file", absDataFile).Str("format", string(importerType)).Msg("Detected file format")
	}
	d.Info("Detected file format: %s", string(importerType))

	// Infer schema from data file
	schema, stats, err := imp.InferSchema(absDataFile)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "import").Str("file", absDataFile).Str("format", string(importerType)).Err(err).Msg("Failed to infer schema")
		}
		d.Error("Failed to infer schema from %s file: %v", string(importerType), err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "import").Str("file", absDataFile).Int("field_count", len(schema.Fields)).Msg("Schema inferred successfully")
	}

	// If just showing inferred schema, print and continue with import
	if importOpts.inferSchema {
		d.Info("Schema inferred from %s:", dataFile)
		printSchemaWithDisplay(schema, d)
		d.Info("File Statistics:")
		printStatsWithDisplay(stats, d)
	}

	// If dry run, show what would be done and exit
	if importOpts.dryRun {
		if logger != nil {
			logger.Info().Str("cmd", "import").Bool("dry_run", true).Msg("Performing dry run")
		}
		d.Info("Dry run - would perform the following operations:")
		d.Info("1. Create namespace: %v", namespaceIdent)
		d.Info("2. Create table: %v", tableIdent)
		d.Info("3. Import from: %s (%s format)", absDataFile, string(importerType))
		d.Info("4. Table location: %s", imp.GetTableLocation(tableIdent))
		d.Info("")
		d.Info("Inferred Schema:")
		printSchemaWithDisplay(schema, d)
		d.Info("")
		d.Info("File Statistics:")
		printStatsWithDisplay(stats, d)
		return nil
	}

	// Perform the actual import
	if logger != nil {
		logger.Info().Str("cmd", "import").Str("file", dataFile).Str("format", string(importerType)).Interface("table", tableIdent).Msg("Starting import")
	}
	d.Info("Importing %s (%s) into table %v...", dataFile, string(importerType), tableIdent)

	result, err := imp.ImportTable(context.Background(), importer.ImportRequest{
		ParquetFile:    absDataFile, // Note: field name is ParquetFile but used for any file type
		TableIdent:     tableIdent,
		NamespaceIdent: namespaceIdent,
		Schema:         schema,
		Overwrite:      importOpts.overwrite,
		PartitionBy:    importOpts.partitionBy,
	})
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "import").Str("file", dataFile).Interface("table", tableIdent).Err(err).Msg("Failed to import table")
		}
		d.Error("Failed to import table: %v", err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "import").Interface("table", result.TableIdent).Int64("records", result.RecordCount).Int64("size_bytes", result.DataSize).Msg("Import completed successfully")
	}

	// Print success message
	d.Success("Successfully imported table!")
	d.Info("Import Results:")
	d.Info("   Table: %v", result.TableIdent)
	d.Info("   Records: %d", result.RecordCount)
	d.Info("   Size: %s", formatBytes(result.DataSize))
	d.Info("   Location: %s", result.TableLocation)
	d.Info("")
	d.Info("Next steps:")
	d.Info("   icebox sql 'SELECT * FROM %s LIMIT 10'", strings.Join(tableIdent, "."))

	return nil
}

// parseTableIdentifier parses table and namespace flags into identifiers
func parseTableIdentifier(tableName, namespace string) (tableIdent table.Identifier, namespaceIdent table.Identifier, err error) {
	if tableName == "" {
		return nil, nil, fmt.Errorf("table name is required")
	}

	// Check if table contains namespace (e.g., "namespace.table")
	if strings.Contains(tableName, ".") {
		if namespace != "" {
			return nil, nil, fmt.Errorf("cannot specify both --namespace flag and namespace in table name")
		}

		parts := strings.Split(tableName, ".")
		if len(parts) != 2 {
			return nil, nil, fmt.Errorf("table name must be in format 'namespace.table' or just 'table'")
		}

		namespaceIdent = table.Identifier{parts[0]}
		tableIdent = table.Identifier{parts[0], parts[1]}
	} else {
		// Use provided namespace or default to "default"
		if namespace == "" {
			namespace = "default"
		}

		namespaceIdent = table.Identifier{namespace}
		tableIdent = table.Identifier{namespace, tableName}
	}

	return tableIdent, namespaceIdent, nil
}

// printSchemaWithDisplay prints the inferred schema using the display package
func printSchemaWithDisplay(schema *importer.Schema, d display.Display) {
	if schema == nil {
		d.Info("  No schema information available")
		return
	}

	d.Info("  Columns (%d):", len(schema.Fields))
	for i, field := range schema.Fields {
		nullable := ""
		if field.Nullable {
			nullable = " (nullable)"
		}
		d.Info("    %d. %s: %s%s", i+1, field.Name, field.Type, nullable)
	}
}

// printStatsWithDisplay prints file statistics using the display package
func printStatsWithDisplay(stats *importer.FileStats, d display.Display) {
	if stats == nil {
		d.Info("  No statistics available")
		return
	}

	d.Info("  Records: %d", stats.RecordCount)
	d.Info("  File size: %s", formatBytes(stats.FileSize))
	d.Info("  Columns: %d", stats.ColumnCount)
}

// formatBytes formats a byte count as a human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}

	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}

	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}
