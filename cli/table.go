package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/TFMV/icebox/catalog"
	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/display"
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var tableCmd = &cobra.Command{
	Use:   "table",
	Short: "Manage Iceberg tables",
	Long: `Manage Iceberg tables in your catalog.

This command provides subcommands for table operations:
- create: Create a new table with a specified schema
- list: List all tables in a namespace
- describe: Show detailed information about a table
- history: Show the snapshot history of a table

Examples:
  icebox table list                           # List tables in default namespace
  icebox table list --namespace analytics     # List tables in specific namespace
  icebox table describe sales                 # Describe a table
  icebox table history sales --max-snapshots 10
  icebox table create test_table --schema schema.json`,
}

var tableListCmd = &cobra.Command{
	Use:   "list",
	Short: "List tables in a namespace",
	Long: `List all tables in the specified namespace.

If no namespace is specified, lists tables in the default namespace.

Examples:
  icebox table list                      # List tables in default namespace
  icebox table list --namespace finance  # List tables in finance namespace
  icebox table list --all-namespaces    # List tables from all namespaces`,
	RunE: runTableList,
}

var tableDescribeCmd = &cobra.Command{
	Use:   "describe <table>",
	Short: "Describe a table's schema and metadata",
	Long: `Show detailed information about a table including:
- Schema (columns, types, nullability)
- Current snapshot information
- Table properties
- Partition specification
- Sort order

Examples:
  icebox table describe sales
  icebox table describe analytics.user_events
  icebox table describe sales --snapshot 1234567890`,
	Args: cobra.ExactArgs(1),
	RunE: runTableDescribe,
}

var tableHistoryCmd = &cobra.Command{
	Use:   "history <table>",
	Short: "Show the snapshot history of a table",
	Long: `Display the complete snapshot history of a table showing:
- Snapshot IDs and timestamps
- Operations that created each snapshot
- Parent-child relationships
- Summary statistics

Examples:
  icebox table history sales
  icebox table history analytics.events --max-snapshots 20
  icebox table history sales --format json`,
	Args: cobra.ExactArgs(1),
	RunE: runTableHistory,
}

var tableCreateCmd = &cobra.Command{
	Use:   "create <table>",
	Short: "Create a new table",
	Long: `Create a new Iceberg table with the specified schema.

The schema can be provided as:
- A JSON file containing the schema definition
- Inline JSON schema specification
- Interactive schema builder (default)

Examples:
  icebox table create sales --schema schema.json
  icebox table create analytics.events --partition-by date
  icebox table create warehouse.inventory --sort-by product_id`,
	Args: cobra.ExactArgs(1),
	RunE: runTableCreate,
}

var tableDropCmd = &cobra.Command{
	Use:   "drop <table>",
	Short: "Drop a table from the catalog",
	Long: `Drop an existing table from the catalog.

This permanently removes the table and all its metadata from the catalog.
The table data files may be left in storage depending on the catalog implementation.

Examples:
  icebox table drop sales
  icebox table drop analytics.user_events
  icebox table drop warehouse.inventory.products`,
	Args: cobra.ExactArgs(1),
	RunE: runTableDrop,
}

type tableListOptions struct {
	namespace      string
	allNamespaces  bool
	format         string
	showProperties bool
}

type tableDescribeOptions struct {
	snapshotID     int64
	format         string
	showProperties bool
	showStats      bool
}

type tableHistoryOptions struct {
	maxSnapshots int
	format       string
	reverse      bool
}

type tableCreateOptions struct {
	schemaFile  string
	schemaJSON  string
	partitionBy []string
	sortBy      []string
	properties  map[string]string
	location    string
}

type tableDropOptions struct {
	force bool
}

var (
	tableListOpts     = &tableListOptions{}
	tableDescribeOpts = &tableDescribeOptions{}
	tableHistoryOpts  = &tableHistoryOptions{}
	tableCreateOpts   = &tableCreateOptions{}
	tableDropOpts     = &tableDropOptions{}
)

func init() {
	rootCmd.AddCommand(tableCmd)

	// Add subcommands
	tableCmd.AddCommand(tableListCmd)
	tableCmd.AddCommand(tableDescribeCmd)
	tableCmd.AddCommand(tableHistoryCmd)
	tableCmd.AddCommand(tableCreateCmd)
	tableCmd.AddCommand(tableDropCmd)

	// Table list flags
	tableListCmd.Flags().StringVar(&tableListOpts.namespace, "namespace", "default", "namespace to list tables from")
	tableListCmd.Flags().BoolVar(&tableListOpts.allNamespaces, "all-namespaces", false, "list tables from all namespaces")
	tableListCmd.Flags().StringVar(&tableListOpts.format, "format", "table", "output format: table, csv, json")
	tableListCmd.Flags().BoolVar(&tableListOpts.showProperties, "show-properties", false, "show table properties")

	// Table describe flags
	tableDescribeCmd.Flags().Int64Var(&tableDescribeOpts.snapshotID, "snapshot", 0, "describe table at specific snapshot ID")
	tableDescribeCmd.Flags().StringVar(&tableDescribeOpts.format, "format", "table", "output format: table, json")
	tableDescribeCmd.Flags().BoolVar(&tableDescribeOpts.showProperties, "show-properties", true, "show table properties")
	tableDescribeCmd.Flags().BoolVar(&tableDescribeOpts.showStats, "show-stats", false, "show table statistics")

	// Table history flags
	tableHistoryCmd.Flags().IntVar(&tableHistoryOpts.maxSnapshots, "max-snapshots", 50, "maximum number of snapshots to show")
	tableHistoryCmd.Flags().StringVar(&tableHistoryOpts.format, "format", "table", "output format: table, json")
	tableHistoryCmd.Flags().BoolVar(&tableHistoryOpts.reverse, "reverse", false, "show oldest snapshots first")

	// Table create flags
	tableCreateCmd.Flags().StringVar(&tableCreateOpts.schemaFile, "schema", "", "path to JSON schema file")
	tableCreateCmd.Flags().StringVar(&tableCreateOpts.schemaJSON, "schema-json", "", "inline JSON schema")
	tableCreateCmd.Flags().StringSliceVar(&tableCreateOpts.partitionBy, "partition-by", nil, "partition columns")
	tableCreateCmd.Flags().StringSliceVar(&tableCreateOpts.sortBy, "sort-by", nil, "sort columns")
	tableCreateCmd.Flags().StringToStringVar(&tableCreateOpts.properties, "property", nil, "table properties (key=value)")
	tableCreateCmd.Flags().StringVar(&tableCreateOpts.location, "location", "", "table location (optional)")

	// Table drop flags
	tableDropCmd.Flags().BoolVar(&tableDropOpts.force, "force", false, "force drop table")
}

func runTableList(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "table-list").Str("namespace", tableListOpts.namespace).Bool("all_namespaces", tableListOpts.allNamespaces).Msg("Starting table list operation")
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-list").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration")
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	if cmd.Flag("verbose") != nil && cmd.Flag("verbose").Value.String() == "true" {
		d.Info("Using configuration: %s", configPath)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-list").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	if tableListOpts.allNamespaces {
		if logger != nil {
			logger.Info().Str("cmd", "table-list").Msg("Listing tables from all namespaces")
		}
		return listTablesAllNamespaces(ctx, cat, d, logger)
	}

	// Parse namespace
	namespace := table.Identifier{tableListOpts.namespace}

	// Check if namespace exists
	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-list").Str("namespace", tableListOpts.namespace).Err(err).Msg("Failed to check namespace existence")
		}
		d.Error("Failed to check namespace existence: %v", err)
		return err
	}
	if !exists {
		if logger != nil {
			logger.Warn().Str("cmd", "table-list").Str("namespace", tableListOpts.namespace).Msg("Namespace does not exist")
		}
		d.Error("Namespace '%s' does not exist", tableListOpts.namespace)
		d.Info("Use 'icebox catalog list' to see available namespaces")
		d.Info("Or create it with: icebox catalog create %s", tableListOpts.namespace)
		return fmt.Errorf("namespace '%s' does not exist", tableListOpts.namespace)
	}

	// List tables in the namespace
	var tables []table.Identifier
	for identifier, err := range cat.ListTables(ctx, namespace) {
		if err != nil {
			if logger != nil {
				logger.Error().Str("cmd", "table-list").Str("namespace", tableListOpts.namespace).Err(err).Msg("Failed to list tables")
			}
			d.Error("Failed to list tables: %v", err)
			return err
		}
		tables = append(tables, identifier)
	}

	if logger != nil {
		logger.Info().Str("cmd", "table-list").Str("namespace", tableListOpts.namespace).Int("table_count", len(tables)).Msg("Successfully listed tables")
	}

	// Display results
	if err := displayTableListWithDisplay(tables, namespace, d); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-list").Err(err).Msg("Failed to display table list")
		}
		d.Error("Failed to display table list: %v", err)
		return err
	}

	return nil
}

func runTableDescribe(cmd *cobra.Command, args []string) error {
	tableName := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "table-describe").Str("table", tableName).Msg("Starting table describe operation")
	}

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-describe").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		return err
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-describe").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-describe").Str("table", tableName).Err(err).Msg("Failed to parse table identifier")
		}
		d.Error("Failed to parse table identifier: %v", err)
		return err
	}

	// Load the table
	icebergTable, err := cat.LoadTable(ctx, tableIdent, nil)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-describe").Str("table", tableName).Err(err).Msg("Failed to load table")
		}
		d.Error("Failed to load table '%s': %v", tableName, err)
		d.Info("Use 'icebox table list' to see available tables")
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "table-describe").Str("table", tableName).Msg("Successfully loaded table")
	}

	// Display table description
	if err := displayTableDescriptionWithDisplay(icebergTable, tableDescribeOpts, d); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-describe").Str("table", tableName).Err(err).Msg("Failed to display table description")
		}
		d.Error("Failed to display table description: %v", err)
		return err
	}

	return nil
}

func runTableHistory(cmd *cobra.Command, args []string) error {
	tableName := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "table-history").Str("table", tableName).Msg("Starting table history operation")
	}

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-history").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		return err
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-history").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-history").Str("table", tableName).Err(err).Msg("Failed to parse table identifier")
		}
		d.Error("Failed to parse table identifier: %v", err)
		return err
	}

	// Load the table
	icebergTable, err := cat.LoadTable(ctx, tableIdent, nil)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-history").Str("table", tableName).Err(err).Msg("Failed to load table")
		}
		d.Error("Failed to load table '%s': %v", tableName, err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "table-history").Str("table", tableName).Msg("Successfully loaded table")
	}

	// Display table history
	if err := displayTableHistoryDetailedWithDisplay(icebergTable, tableHistoryOpts, d); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-history").Str("table", tableName).Err(err).Msg("Failed to display table history")
		}
		d.Error("Failed to display table history: %v", err)
		return err
	}

	return nil
}

func runTableCreate(cmd *cobra.Command, args []string) error {
	tableName := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "table-create").Str("table", tableName).Msg("Starting table create operation")
	}

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		return err
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, namespaceIdent, err := parseTableIdentifier(tableName, "")
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Str("table", tableName).Err(err).Msg("Failed to parse table identifier")
		}
		d.Error("Failed to parse table identifier: %v", err)
		return err
	}

	// Ensure namespace exists
	exists, err := cat.CheckNamespaceExists(ctx, namespaceIdent)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Interface("namespace", namespaceIdent).Err(err).Msg("Failed to check namespace existence")
		}
		d.Error("Failed to check namespace existence: %v", err)
		return err
	}
	if !exists {
		if err := cat.CreateNamespace(ctx, namespaceIdent, iceberg.Properties{}); err != nil {
			if logger != nil {
				logger.Error().Str("cmd", "table-create").Interface("namespace", namespaceIdent).Err(err).Msg("Failed to create namespace")
			}
			d.Error("Failed to create namespace: %v", err)
			return err
		}
		if logger != nil {
			logger.Info().Str("cmd", "table-create").Interface("namespace", namespaceIdent).Msg("Created namespace")
		}
		d.Success("Created namespace: %v", namespaceIdent)
	}

	// Get schema
	schema, err := getTableSchema(tableCreateOpts)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Err(err).Msg("Failed to get table schema")
		}
		d.Error("Failed to get table schema: %v", err)
		return err
	}

	// Create partition specification
	partitionSpec, err := createPartitionSpec(schema, tableCreateOpts.partitionBy)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Err(err).Msg("Failed to create partition specification")
		}
		d.Error("Failed to create partition specification: %v", err)
		return err
	}

	// Create sort order
	sortOrder, err := createSortOrder(schema, tableCreateOpts.sortBy)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Err(err).Msg("Failed to create sort order")
		}
		d.Error("Failed to create sort order: %v", err)
		return err
	}

	// Prepare table properties
	properties := iceberg.Properties{}
	for key, value := range tableCreateOpts.properties {
		properties[key] = value
	}

	// Set location if specified
	if tableCreateOpts.location != "" {
		properties["location"] = tableCreateOpts.location
	}

	// Create the table with comprehensive options
	createdTable, err := createTableWithOptions(ctx, cat, tableIdent, schema, partitionSpec, sortOrder, properties)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-create").Str("table", tableName).Err(err).Msg("Failed to create table")
		}
		d.Error("Failed to create table: %v", err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "table-create").Str("table", tableName).Interface("table_id", tableIdent).Msg("Successfully created table")
	}

	// Display success message
	d.Success("Successfully created table!")
	d.Info("Table Details:")
	d.Info("   Name: %v", tableIdent)
	d.Info("   Location: %s", createdTable.Location())
	d.Info("   Schema ID: %d", createdTable.Schema().ID)
	d.Info("   Columns: %d", len(createdTable.Schema().Fields()))

	// Show partition info if partitioned
	if len(tableCreateOpts.partitionBy) > 0 {
		d.Info("   Partitioned by: %s", strings.Join(tableCreateOpts.partitionBy, ", "))
	}

	// Show sort info if sorted
	if len(tableCreateOpts.sortBy) > 0 {
		d.Info("   Sorted by: %s", strings.Join(tableCreateOpts.sortBy, ", "))
	}

	// Show properties if any
	if len(properties) > 0 {
		d.Info("   Properties:")
		for key, value := range properties {
			d.Info("     %s: %s", key, value)
		}
	}

	return nil
}

func runTableDrop(cmd *cobra.Command, args []string) error {
	tableName := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "table-drop").Str("table", tableName).Msg("Starting table drop operation")
	}

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-drop").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		return err
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-drop").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-drop").Str("table", tableName).Err(err).Msg("Failed to parse table identifier")
		}
		d.Error("Failed to parse table identifier: %v", err)
		return err
	}

	// Drop the table
	if err := cat.DropTable(ctx, tableIdent); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-drop").Str("table", tableName).Err(err).Msg("Failed to drop table")
		}
		d.Error("Failed to drop table: %v", err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "table-drop").Str("table", tableName).Interface("table_id", tableIdent).Msg("Successfully dropped table")
	}

	d.Success("Successfully dropped table!")
	return nil
}

// Helper functions for table commands

func listTablesAllNamespaces(ctx context.Context, cat catalog.CatalogInterface, d display.Display, logger *zerolog.Logger) error {
	// Get all namespaces
	namespaces, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "table-list").Err(err).Msg("Failed to list namespaces")
		}
		d.Error("Failed to list namespaces: %v", err)
		return err
	}

	var allTables []table.Identifier
	for _, namespace := range namespaces {
		for identifier, err := range cat.ListTables(ctx, namespace) {
			if err != nil {
				if logger != nil {
					logger.Error().Str("cmd", "table-list").Interface("namespace", namespace).Err(err).Msg("Failed to list tables in namespace")
				}
				d.Error("Failed to list tables in namespace %v: %v", namespace, err)
				return err
			}
			allTables = append(allTables, identifier)
		}
	}

	if logger != nil {
		logger.Info().Str("cmd", "table-list").Int("namespace_count", len(namespaces)).Int("total_tables", len(allTables)).Msg("Successfully listed tables from all namespaces")
	}

	return displayTableListWithDisplay(allTables, nil, d)
}

func displayTableListWithDisplay(tables []table.Identifier, namespace table.Identifier, d display.Display) error {
	if len(tables) == 0 {
		if namespace != nil {
			d.Info("No tables found in namespace '%s'", strings.Join(namespace, "."))
		} else {
			d.Info("No tables found")
		}
		return nil
	}

	switch tableListOpts.format {
	case "table":
		return displayTableListTableWithDisplay(tables, namespace, d)
	case "csv":
		return displayTableListCSVWithDisplay(tables, d)
	case "json":
		return displayTableListJSONWithDisplay(tables, d)
	default:
		d.Error("Unsupported format: %s", tableListOpts.format)
		return fmt.Errorf("unsupported format: %s", tableListOpts.format)
	}
}

// Legacy function for backward compatibility
func displayTableList(tables []table.Identifier, namespace table.Identifier) error {
	d := display.New()
	return displayTableListWithDisplay(tables, namespace, d)
}

func displayTableListTableWithDisplay(tables []table.Identifier, namespace table.Identifier, d display.Display) error {
	// Prepare table data
	headers := []string{"Namespace", "Table"}
	var rows [][]interface{}

	for _, tableIdent := range tables {
		namespace := "default"
		tableName := strings.Join(tableIdent, ".")
		if len(tableIdent) > 1 {
			namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
			tableName = tableIdent[len(tableIdent)-1]
		}
		rows = append(rows, []interface{}{namespace, tableName})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	title := fmt.Sprintf("All Tables (%d tables)", len(tables))
	if namespace != nil {
		title = fmt.Sprintf("Tables in namespace '%s' (%d tables)", strings.Join(namespace, "."), len(tables))
	}

	return d.Table(tableData).WithTitle(title).Render()
}

// Legacy function for backward compatibility
func displayTableListTable(tables []table.Identifier, namespace table.Identifier) error {
	d := display.New()
	return displayTableListTableWithDisplay(tables, namespace, d)
}

func displayTableListCSVWithDisplay(tables []table.Identifier, d display.Display) error {
	// Prepare table data
	headers := []string{"namespace", "table"}
	var rows [][]interface{}

	for _, tableIdent := range tables {
		namespace := "default"
		tableName := strings.Join(tableIdent, ".")
		if len(tableIdent) > 1 {
			namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
			tableName = tableIdent[len(tableIdent)-1]
		}
		rows = append(rows, []interface{}{namespace, tableName})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	return d.Table(tableData).WithFormat(display.FormatCSV).Render()
}

// Legacy function for backward compatibility
func displayTableListCSV(tables []table.Identifier) error {
	d := display.New()
	return displayTableListCSVWithDisplay(tables, d)
}

func displayTableListJSONWithDisplay(tables []table.Identifier, d display.Display) error {
	// Prepare table data
	headers := []string{"namespace", "table"}
	var rows [][]interface{}

	for _, tableIdent := range tables {
		namespace := "default"
		tableName := strings.Join(tableIdent, ".")
		if len(tableIdent) > 1 {
			namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
			tableName = tableIdent[len(tableIdent)-1]
		}
		rows = append(rows, []interface{}{namespace, tableName})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	return d.Table(tableData).WithFormat(display.FormatJSON).Render()
}

// Legacy function for backward compatibility
func displayTableListJSON(tables []table.Identifier) error {
	d := display.New()
	return displayTableListJSONWithDisplay(tables, d)
}

func displayTableDescriptionWithDisplay(tbl *table.Table, opts *tableDescribeOptions, d display.Display) error {
	d.Info("Table: %v", tbl.Identifier())
	d.Info("Location: %s", tbl.Location())
	d.Info("Format Version: %d", tbl.Metadata().Version())

	// Schema information
	schema := tbl.Schema()
	d.Info("Schema (ID: %d):", schema.ID)

	// Prepare schema table data
	headers := []string{"#", "Name", "Type", "Required"}
	var rows [][]interface{}

	for _, field := range schema.Fields() {
		required := "Yes"
		if !field.Required {
			required = "No"
		}
		rows = append(rows, []interface{}{
			field.ID,
			field.Name,
			field.Type.String(),
			required,
		})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	if err := d.Table(tableData).Render(); err != nil {
		return err
	}

	// Current snapshot information
	if currentSnapshot := tbl.CurrentSnapshot(); currentSnapshot != nil {
		d.Info("Current Snapshot: %d", currentSnapshot.SnapshotID)
		d.Info("Timestamp: %s", time.UnixMilli(currentSnapshot.TimestampMs).Format("2006-01-02 15:04:05"))
		if currentSnapshot.Summary != nil {
			d.Info("Operation: %s", currentSnapshot.Summary.Operation)
		}
	}

	// Partition spec
	spec := tbl.Spec()
	var hasPartitionFields bool
	for range spec.Fields() {
		hasPartitionFields = true
		break // Just check if there are any fields
	}
	if hasPartitionFields {
		d.Info("Partition Spec (ID: %d):", spec.ID())
		for field := range spec.Fields() {
			d.Info("   - %s", field.String())
		}
	}

	// Sort order
	if sortOrder := tbl.SortOrder(); len(sortOrder.Fields) > 0 {
		d.Info("Sort Order (ID: %d):", sortOrder.OrderID)
		for _, field := range sortOrder.Fields {
			d.Info("   - %s", field.String())
		}
	}

	// Properties
	if opts.showProperties {
		props := tbl.Properties()
		if len(props) > 0 {
			d.Info("Properties:")
			for key, value := range props {
				d.Info("   %s: %s", key, value)
			}
		}
	}

	return nil
}

// Legacy function for backward compatibility
func displayTableDescription(tbl *table.Table, opts *tableDescribeOptions) error {
	d := display.New()
	return displayTableDescriptionWithDisplay(tbl, opts, d)
}

func displayTableHistoryDetailedWithDisplay(tbl *table.Table, opts *tableHistoryOptions, d display.Display) error {
	snapshots := tbl.Metadata().Snapshots()
	if len(snapshots) == 0 {
		d.Info("No snapshots found in table history")
		return nil
	}

	// Limit snapshots if requested
	displaySnapshots := snapshots
	if opts.maxSnapshots > 0 && len(snapshots) > opts.maxSnapshots {
		if opts.reverse {
			displaySnapshots = snapshots[:opts.maxSnapshots]
		} else {
			displaySnapshots = snapshots[len(snapshots)-opts.maxSnapshots:]
		}
		d.Info("Table History (showing %d of %d snapshots):", len(displaySnapshots), len(snapshots))
	} else {
		d.Info("Table History (%d snapshots):", len(displaySnapshots))
	}

	switch opts.format {
	case "table":
		return displayTableHistoryTableWithDisplay(tbl, displaySnapshots, opts.reverse, d)
	case "json":
		return displayTableHistoryJSONWithDisplay(displaySnapshots, opts.reverse, d)
	default:
		d.Error("Unsupported format: %s", opts.format)
		return fmt.Errorf("unsupported format: %s", opts.format)
	}
}

// Legacy function for backward compatibility
func displayTableHistoryDetailed(tbl *table.Table, opts *tableHistoryOptions) error {
	d := display.New()
	return displayTableHistoryDetailedWithDisplay(tbl, opts, d)
}

func displayTableHistoryTableWithDisplay(tbl *table.Table, snapshots []table.Snapshot, reverse bool, d display.Display) error {
	// Prepare table data
	headers := []string{"Snapshot ID", "Timestamp", "Operation", "Parent ID", "Records"}
	var rows [][]interface{}

	// Display order
	displayOrder := snapshots
	if !reverse {
		// Reverse to show newest first (default)
		displayOrder = make([]table.Snapshot, len(snapshots))
		for i, snapshot := range snapshots {
			displayOrder[len(snapshots)-1-i] = snapshot
		}
	}

	for _, snapshot := range displayOrder {
		timestamp := time.UnixMilli(snapshot.TimestampMs).Format("2006-01-02 15:04:05")

		operation := "unknown"
		if snapshot.Summary != nil {
			operation = string(snapshot.Summary.Operation)
		}

		parentID := "none"
		if snapshot.ParentSnapshotID != nil {
			parentID = fmt.Sprintf("%d", *snapshot.ParentSnapshotID)
		}

		records := "unknown"
		if snapshot.Summary != nil && snapshot.Summary.Properties != nil {
			if recordCount, exists := snapshot.Summary.Properties["added-records"]; exists {
				records = recordCount
			}
		}

		// Mark current snapshot
		snapshotIDStr := fmt.Sprintf("%d", snapshot.SnapshotID)
		if tbl.CurrentSnapshot() != nil && snapshot.SnapshotID == tbl.CurrentSnapshot().SnapshotID {
			snapshotIDStr = "*" + snapshotIDStr
		}

		rows = append(rows, []interface{}{
			snapshotIDStr,
			timestamp,
			operation,
			parentID,
			records,
		})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	if err := d.Table(tableData).Render(); err != nil {
		return err
	}

	d.Info("* = current snapshot")
	return nil
}

func displayTableHistoryJSONWithDisplay(snapshots []table.Snapshot, reverse bool, d display.Display) error {
	// Prepare table data for JSON output
	headers := []string{"snapshot_id", "timestamp", "operation", "parent_snapshot_id"}
	var rows [][]interface{}

	// Display order
	displayOrder := snapshots
	if !reverse {
		// Reverse to show newest first (default)
		displayOrder = make([]table.Snapshot, len(snapshots))
		for i, snapshot := range snapshots {
			displayOrder[len(snapshots)-1-i] = snapshot
		}
	}

	for _, snapshot := range displayOrder {
		timestamp := time.UnixMilli(snapshot.TimestampMs).Format(time.RFC3339)

		operation := "unknown"
		if snapshot.Summary != nil {
			operation = string(snapshot.Summary.Operation)
		}

		var parentID interface{} = nil
		if snapshot.ParentSnapshotID != nil {
			parentID = *snapshot.ParentSnapshotID
		}

		rows = append(rows, []interface{}{
			snapshot.SnapshotID,
			timestamp,
			operation,
			parentID,
		})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	return d.Table(tableData).WithFormat(display.FormatJSON).Render()
}

// Legacy function for backward compatibility
func displayTableHistoryJSON(snapshots []table.Snapshot, reverse bool) error {
	d := display.New()
	return displayTableHistoryJSONWithDisplay(snapshots, reverse, d)
}

func getTableSchema(opts *tableCreateOptions) (*iceberg.Schema, error) {
	if opts.schemaFile != "" {
		return readSchemaFromFile(opts.schemaFile)
	}

	if opts.schemaJSON != "" {
		return parseSchemaFromJSON(opts.schemaJSON)
	}

	// Interactive schema builder or default schema
	return createDefaultSchema(), nil
}

func readSchemaFromFile(filename string) (*iceberg.Schema, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	return parseSchemaFromJSON(string(data))
}

func parseSchemaFromJSON(schemaJSON string) (*iceberg.Schema, error) {
	// Define the JSON schema structure
	type JSONField struct {
		ID       int    `json:"id"`
		Name     string `json:"name"`
		Type     string `json:"type"`
		Required bool   `json:"required"`
	}

	type JSONSchema struct {
		Type   string      `json:"type"`
		Fields []JSONField `json:"fields"`
	}

	// Parse the JSON
	var jsonSchema JSONSchema
	if err := json.Unmarshal([]byte(schemaJSON), &jsonSchema); err != nil {
		return nil, fmt.Errorf("failed to parse JSON schema: %w", err)
	}

	// Validate schema type
	if jsonSchema.Type != "struct" {
		return nil, fmt.Errorf("schema type must be 'struct', got '%s'", jsonSchema.Type)
	}

	// Convert JSON fields to Iceberg fields
	var icebergFields []iceberg.NestedField
	for _, field := range jsonSchema.Fields {
		icebergType, err := parseIcebergType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to parse type for field '%s': %w", field.Name, err)
		}

		icebergField := iceberg.NestedField{
			ID:       field.ID,
			Name:     field.Name,
			Type:     icebergType,
			Required: field.Required,
		}
		icebergFields = append(icebergFields, icebergField)
	}

	// Create and return the schema
	return iceberg.NewSchema(0, icebergFields...), nil
}

// parseIcebergType converts a string type to an Iceberg type
func parseIcebergType(typeStr string) (iceberg.Type, error) {
	switch strings.ToLower(typeStr) {
	case "boolean", "bool":
		return iceberg.PrimitiveTypes.Bool, nil
	case "int", "integer", "int32":
		return iceberg.PrimitiveTypes.Int32, nil
	case "long", "int64", "bigint":
		return iceberg.PrimitiveTypes.Int64, nil
	case "float", "float32":
		return iceberg.PrimitiveTypes.Float32, nil
	case "double", "float64":
		return iceberg.PrimitiveTypes.Float64, nil
	case "decimal":
		// Default decimal(38,18) - could be enhanced to parse precision/scale
		return iceberg.DecimalTypeOf(38, 18), nil
	case "date":
		return iceberg.PrimitiveTypes.Date, nil
	case "time":
		return iceberg.PrimitiveTypes.Time, nil
	case "timestamp":
		return iceberg.PrimitiveTypes.Timestamp, nil
	case "timestamptz", "timestamp_tz":
		return iceberg.PrimitiveTypes.TimestampTz, nil
	case "string", "varchar", "text":
		return iceberg.PrimitiveTypes.String, nil
	case "uuid":
		return iceberg.PrimitiveTypes.UUID, nil
	case "fixed":
		// Default fixed(16) - could be enhanced to parse length
		return iceberg.FixedTypeOf(16), nil
	case "binary":
		return iceberg.PrimitiveTypes.Binary, nil
	default:
		// Try to parse complex types like decimal(10,2), fixed(8), etc.
		if strings.HasPrefix(typeStr, "decimal(") && strings.HasSuffix(typeStr, ")") {
			// Parse decimal(precision,scale)
			params := strings.TrimPrefix(strings.TrimSuffix(typeStr, ")"), "decimal(")
			parts := strings.Split(params, ",")
			if len(parts) == 2 {
				var precision, scale int
				if _, err := fmt.Sscanf(parts[0], "%d", &precision); err == nil {
					if _, err := fmt.Sscanf(parts[1], "%d", &scale); err == nil {
						return iceberg.DecimalTypeOf(precision, scale), nil
					}
				}
			}
		}

		if strings.HasPrefix(typeStr, "fixed(") && strings.HasSuffix(typeStr, ")") {
			// Parse fixed(length)
			lengthStr := strings.TrimPrefix(strings.TrimSuffix(typeStr, ")"), "fixed(")
			var length int
			if _, err := fmt.Sscanf(lengthStr, "%d", &length); err == nil {
				return iceberg.FixedTypeOf(length), nil
			}
		}

		return nil, fmt.Errorf("unsupported type: %s", typeStr)
	}
}

func createDefaultSchema() *iceberg.Schema {
	// Create a simple default schema for demonstration
	fields := []iceberg.NestedField{
		{ID: 1, Name: "id", Type: iceberg.PrimitiveTypes.Int64, Required: true},
		{ID: 2, Name: "name", Type: iceberg.PrimitiveTypes.String, Required: false},
		{ID: 3, Name: "created_at", Type: iceberg.PrimitiveTypes.TimestampTz, Required: false},
	}

	return iceberg.NewSchema(0, fields...)
}

// createTableWithOptions creates a table with comprehensive options
func createTableWithOptions(ctx context.Context, cat catalog.CatalogInterface,
	tableIdent table.Identifier, schema *iceberg.Schema,
	partitionSpec *iceberg.PartitionSpec, sortOrder *table.SortOrder,
	properties iceberg.Properties) (*table.Table, error) {

	// Create catalog options for table creation
	var opts []icebergcatalog.CreateTableOpt

	// Add partition specification if provided
	if partitionSpec != nil && !partitionSpec.IsUnpartitioned() {
		opts = append(opts, icebergcatalog.WithPartitionSpec(partitionSpec))
	}

	// Add sort order if provided
	if sortOrder != nil && len(sortOrder.Fields) > 0 {
		opts = append(opts, icebergcatalog.WithSortOrder(*sortOrder))
	}

	// Add properties if provided
	if len(properties) > 0 {
		opts = append(opts, icebergcatalog.WithProperties(properties))
	}

	// Create table with all options
	createdTable, err := cat.CreateTable(ctx, tableIdent, schema, opts...)
	if err != nil {
		return nil, err
	}

	// Note: Logging of applied specifications is now handled in the calling function

	return createdTable, nil
}

// createPartitionSpec creates a partition specification from column names
func createPartitionSpec(schema *iceberg.Schema, partitionColumns []string) (*iceberg.PartitionSpec, error) {
	if len(partitionColumns) == 0 {
		spec := iceberg.NewPartitionSpec()
		return &spec, nil
	}

	// Create partition fields for the specified columns
	var partitionFields []iceberg.PartitionField
	fieldID := 1000 // Start partition field IDs at 1000

	for _, colName := range partitionColumns {
		// Find the field in the schema to validate it exists
		var found bool
		for _, field := range schema.Fields() {
			if field.Name == colName {
				// Create partition field with identity transform
				partitionField := iceberg.PartitionField{
					SourceID:  field.ID,
					FieldID:   fieldID,
					Transform: iceberg.IdentityTransform{},
					Name:      colName,
				}
				partitionFields = append(partitionFields, partitionField)
				fieldID++
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("partition column '%s' not found in schema", colName)
		}
	}

	// Create partition spec with fields
	spec := iceberg.NewPartitionSpec(partitionFields...)
	return &spec, nil
}

// createSortOrder creates a sort order from column names
func createSortOrder(schema *iceberg.Schema, sortColumns []string) (*table.SortOrder, error) {
	if len(sortColumns) == 0 {
		return &table.UnsortedSortOrder, nil
	}

	var sortFields []table.SortField

	for _, colName := range sortColumns {
		// Parse direction if specified (e.g., "name DESC" or "age ASC")
		parts := strings.Fields(colName)
		columnName := parts[0]
		direction := table.SortASC // default

		if len(parts) > 1 {
			upperDir := strings.ToUpper(parts[1])
			if upperDir == "DESC" {
				direction = table.SortDESC
			} else if upperDir == "ASC" {
				direction = table.SortASC
			}
		}

		// Find the field in the schema
		var found bool
		for _, field := range schema.Fields() {
			if field.Name == columnName {
				nullOrder := table.NullsLast
				if direction == table.SortASC {
					nullOrder = table.NullsFirst
				}

				sortField := table.SortField{
					SourceID:  field.ID,
					Transform: iceberg.IdentityTransform{},
					Direction: direction,
					NullOrder: nullOrder,
				}
				sortFields = append(sortFields, sortField)
				found = true
				break
			}
		}

		if !found {
			return nil, fmt.Errorf("sort column '%s' not found in schema", columnName)
		}
	}

	return &table.SortOrder{
		OrderID: table.InitialSortOrderID,
		Fields:  sortFields,
	}, nil
}
