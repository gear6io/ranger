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
	"github.com/apache/iceberg-go"
	icebergcatalog "github.com/apache/iceberg-go/catalog"
	"github.com/apache/iceberg-go/table"
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
	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", err)
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	ctx := cmd.Context()

	if tableListOpts.allNamespaces {
		return listTablesAllNamespaces(ctx, cat)
	}

	// Parse namespace
	namespace := table.Identifier{tableListOpts.namespace}

	// Check if namespace exists
	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		return fmt.Errorf("❌ Failed to check namespace existence: %w", err)
	}
	if !exists {
		return fmt.Errorf("❌ Namespace '%s' does not exist\n"+
			"💡 Use 'icebox catalog list' to see available namespaces", tableListOpts.namespace)
	}

	// List tables in the namespace
	var tables []table.Identifier
	for identifier, err := range cat.ListTables(ctx, namespace) {
		if err != nil {
			return fmt.Errorf("❌ Failed to list tables: %w", err)
		}
		tables = append(tables, identifier)
	}

	// Display results
	if err := displayTableList(tables, namespace); err != nil {
		return fmt.Errorf("❌ Failed to display table list: %w", err)
	}

	return nil
}

func runTableDescribe(cmd *cobra.Command, args []string) error {
	tableName := args[0]

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration: %w", err)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		return fmt.Errorf("❌ Failed to parse table identifier: %w", err)
	}

	// Load the table
	icebergTable, err := cat.LoadTable(cmd.Context(), tableIdent, nil)
	if err != nil {
		return fmt.Errorf("❌ Failed to load table '%s': %w\n"+
			"💡 Use 'icebox table list' to see available tables", tableName, err)
	}

	// Display table description
	if err := displayTableDescription(icebergTable, tableDescribeOpts); err != nil {
		return fmt.Errorf("❌ Failed to display table description: %w", err)
	}

	return nil
}

func runTableHistory(cmd *cobra.Command, args []string) error {
	tableName := args[0]

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration: %w", err)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		return fmt.Errorf("❌ Failed to parse table identifier: %w", err)
	}

	// Load the table
	icebergTable, err := cat.LoadTable(cmd.Context(), tableIdent, nil)
	if err != nil {
		return fmt.Errorf("❌ Failed to load table '%s': %w", tableName, err)
	}

	// Display table history
	if err := displayTableHistoryDetailed(icebergTable, tableHistoryOpts); err != nil {
		return fmt.Errorf("❌ Failed to display table history: %w", err)
	}

	return nil
}

func runTableCreate(cmd *cobra.Command, args []string) error {
	tableName := args[0]

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration: %w", err)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, namespaceIdent, err := parseTableIdentifier(tableName, "")
	if err != nil {
		return fmt.Errorf("❌ Failed to parse table identifier: %w", err)
	}

	// Ensure namespace exists
	exists, err := cat.CheckNamespaceExists(cmd.Context(), namespaceIdent)
	if err != nil {
		return fmt.Errorf("❌ Failed to check namespace existence: %w", err)
	}
	if !exists {
		if err := cat.CreateNamespace(cmd.Context(), namespaceIdent, iceberg.Properties{}); err != nil {
			return fmt.Errorf("❌ Failed to create namespace: %w", err)
		}
		fmt.Printf("✅ Created namespace: %v\n", namespaceIdent)
	}

	// Get schema
	schema, err := getTableSchema(tableCreateOpts)
	if err != nil {
		return fmt.Errorf("❌ Failed to get table schema: %w", err)
	}

	// Create partition specification
	partitionSpec, err := createPartitionSpec(schema, tableCreateOpts.partitionBy)
	if err != nil {
		return fmt.Errorf("❌ Failed to create partition specification: %w", err)
	}

	// Create sort order
	sortOrder, err := createSortOrder(schema, tableCreateOpts.sortBy)
	if err != nil {
		return fmt.Errorf("❌ Failed to create sort order: %w", err)
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
	createdTable, err := createTableWithOptions(cmd.Context(), cat, tableIdent, schema, partitionSpec, sortOrder, properties)
	if err != nil {
		return fmt.Errorf("❌ Failed to create table: %w", err)
	}

	// Display success message
	fmt.Printf("✅ Successfully created table!\n\n")
	fmt.Printf("📊 Table Details:\n")
	fmt.Printf("   Name: %v\n", tableIdent)
	fmt.Printf("   Location: %s\n", createdTable.Location())
	fmt.Printf("   Schema ID: %d\n", createdTable.Schema().ID)
	fmt.Printf("   Columns: %d\n", len(createdTable.Schema().Fields()))

	// Show partition info if partitioned
	if len(tableCreateOpts.partitionBy) > 0 {
		fmt.Printf("   Partitioned by: %s\n", strings.Join(tableCreateOpts.partitionBy, ", "))
	}

	// Show sort info if sorted
	if len(tableCreateOpts.sortBy) > 0 {
		fmt.Printf("   Sorted by: %s\n", strings.Join(tableCreateOpts.sortBy, ", "))
	}

	// Show properties if any
	if len(properties) > 0 {
		fmt.Printf("   Properties:\n")
		for key, value := range properties {
			fmt.Printf("     %s: %s\n", key, value)
		}
	}

	return nil
}

func runTableDrop(cmd *cobra.Command, args []string) error {
	tableName := args[0]

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration: %w", err)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Parse table identifier
	tableIdent, _, err := parseTableIdentifier(tableName, "")
	if err != nil {
		return fmt.Errorf("❌ Failed to parse table identifier: %w", err)
	}

	// Drop the table
	if err := cat.DropTable(cmd.Context(), tableIdent); err != nil {
		return fmt.Errorf("❌ Failed to drop table: %w", err)
	}

	fmt.Printf("✅ Successfully dropped table!\n")
	return nil
}

// Helper functions for table commands

func listTablesAllNamespaces(ctx context.Context, cat catalog.CatalogInterface) error {
	// Get all namespaces
	namespaces, err := cat.ListNamespaces(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to list namespaces: %w", err)
	}

	var allTables []table.Identifier
	for _, namespace := range namespaces {
		for identifier, err := range cat.ListTables(ctx, namespace) {
			if err != nil {
				return fmt.Errorf("failed to list tables in namespace %v: %w", namespace, err)
			}
			allTables = append(allTables, identifier)
		}
	}

	return displayTableList(allTables, nil)
}

func displayTableList(tables []table.Identifier, namespace table.Identifier) error {
	if len(tables) == 0 {
		if namespace != nil {
			fmt.Printf("📭 No tables found in namespace '%s'\n", strings.Join(namespace, "."))
		} else {
			fmt.Println("📭 No tables found")
		}
		return nil
	}

	switch tableListOpts.format {
	case "table":
		return displayTableListTable(tables, namespace)
	case "csv":
		return displayTableListCSV(tables)
	case "json":
		return displayTableListJSON(tables)
	default:
		return fmt.Errorf("unsupported format: %s", tableListOpts.format)
	}
}

func displayTableListTable(tables []table.Identifier, namespace table.Identifier) error {
	if namespace != nil {
		fmt.Printf("📊 Tables in namespace '%s' (%d tables):\n", strings.Join(namespace, "."), len(tables))
	} else {
		fmt.Printf("📊 All Tables (%d tables):\n", len(tables))
	}

	fmt.Println("┌────────────────────────────────┬──────────────────────────────────┐")
	fmt.Println("│           Namespace            │             Table                │")
	fmt.Println("├────────────────────────────────┼──────────────────────────────────┤")

	for _, tableIdent := range tables {
		namespace := "default"
		tableName := strings.Join(tableIdent, ".")
		if len(tableIdent) > 1 {
			namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
			tableName = tableIdent[len(tableIdent)-1]
		}

		fmt.Printf("│ %-30s │ %-32s │\n", truncateString(namespace, 30), truncateString(tableName, 32))
	}

	fmt.Println("└────────────────────────────────┴──────────────────────────────────┘")
	return nil
}

func displayTableListCSV(tables []table.Identifier) error {
	fmt.Println("namespace,table")
	for _, tableIdent := range tables {
		namespace := "default"
		tableName := strings.Join(tableIdent, ".")
		if len(tableIdent) > 1 {
			namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
			tableName = tableIdent[len(tableIdent)-1]
		}
		fmt.Printf("%s,%s\n", namespace, tableName)
	}
	return nil
}

func displayTableListJSON(tables []table.Identifier) error {
	fmt.Println("[")
	for i, tableIdent := range tables {
		namespace := "default"
		tableName := strings.Join(tableIdent, ".")
		if len(tableIdent) > 1 {
			namespace = strings.Join(tableIdent[:len(tableIdent)-1], ".")
			tableName = tableIdent[len(tableIdent)-1]
		}

		fmt.Printf(`  {"namespace": "%s", "table": "%s"}`, namespace, tableName)
		if i < len(tables)-1 {
			fmt.Print(",")
		}
		fmt.Println()
	}
	fmt.Println("]")
	return nil
}

func displayTableDescription(tbl *table.Table, opts *tableDescribeOptions) error {
	fmt.Printf("📊 Table: %v\n", tbl.Identifier())
	fmt.Printf("📍 Location: %s\n", tbl.Location())
	fmt.Printf("🔗 Format Version: %d\n", tbl.Metadata().Version())
	fmt.Println()

	// Schema information
	schema := tbl.Schema()
	fmt.Printf("📋 Schema (ID: %d):\n", schema.ID)
	fmt.Println("┌────┬─────────────────────────────┬──────────────────────┬──────────┐")
	fmt.Println("│ #  │           Name              │        Type          │ Required │")
	fmt.Println("├────┼─────────────────────────────┼──────────────────────┼──────────┤")

	for _, field := range schema.Fields() {
		required := "Yes"
		if !field.Required {
			required = "No"
		}
		fmt.Printf("│%-3d │ %-27s │ %-20s │ %-8s │\n",
			field.ID,
			truncateString(field.Name, 27),
			truncateString(field.Type.String(), 20),
			required)
	}
	fmt.Println("└────┴─────────────────────────────┴──────────────────────┴──────────┘")

	// Current snapshot information
	if currentSnapshot := tbl.CurrentSnapshot(); currentSnapshot != nil {
		fmt.Printf("\n📸 Current Snapshot: %d\n", currentSnapshot.SnapshotID)
		fmt.Printf("⏰ Timestamp: %s\n", time.UnixMilli(currentSnapshot.TimestampMs).Format("2006-01-02 15:04:05"))
		if currentSnapshot.Summary != nil {
			fmt.Printf("🔄 Operation: %s\n", currentSnapshot.Summary.Operation)
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
		fmt.Printf("\n🗂️  Partition Spec (ID: %d):\n", spec.ID())
		for field := range spec.Fields() {
			fmt.Printf("   - %s\n", field.String())
		}
	}

	// Sort order
	if sortOrder := tbl.SortOrder(); len(sortOrder.Fields) > 0 {
		fmt.Printf("\n🔄 Sort Order (ID: %d):\n", sortOrder.OrderID)
		for _, field := range sortOrder.Fields {
			fmt.Printf("   - %s\n", field.String())
		}
	}

	// Properties
	if opts.showProperties {
		props := tbl.Properties()
		if len(props) > 0 {
			fmt.Printf("\n⚙️  Properties:\n")
			for key, value := range props {
				fmt.Printf("   %s: %s\n", key, value)
			}
		}
	}

	return nil
}

func displayTableHistoryDetailed(tbl *table.Table, opts *tableHistoryOptions) error {
	snapshots := tbl.Metadata().Snapshots()
	if len(snapshots) == 0 {
		fmt.Println("📭 No snapshots found in table history")
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
		fmt.Printf("📚 Table History (showing %d of %d snapshots):\n", len(displaySnapshots), len(snapshots))
	} else {
		fmt.Printf("📚 Table History (%d snapshots):\n", len(displaySnapshots))
	}

	switch opts.format {
	case "table":
		return displayTableHistoryTable(tbl, displaySnapshots, opts.reverse)
	case "json":
		return displayTableHistoryJSON(displaySnapshots, opts.reverse)
	default:
		return fmt.Errorf("unsupported format: %s", opts.format)
	}
}

func displayTableHistoryTable(tbl *table.Table, snapshots []table.Snapshot, reverse bool) error {
	fmt.Println("┌────────────────────┬─────────────────────────┬─────────────────────┬──────────────┬─────────────┐")
	fmt.Println("│    Snapshot ID     │       Timestamp         │      Operation      │  Parent ID   │   Records   │")
	fmt.Println("├────────────────────┼─────────────────────────┼─────────────────────┼──────────────┼─────────────┤")

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

		// Truncate long IDs for better display
		snapshotIDStr := fmt.Sprintf("%d", snapshot.SnapshotID)
		if len(snapshotIDStr) > 18 {
			snapshotIDStr = snapshotIDStr[:15] + "..."
		}

		if len(parentID) > 12 && parentID != "none" {
			parentID = parentID[:9] + "..."
		}

		// Mark current snapshot
		marker := " "
		if tbl.CurrentSnapshot() != nil && snapshot.SnapshotID == tbl.CurrentSnapshot().SnapshotID {
			marker = "*"
		}

		fmt.Printf("│%s%-18s │ %-23s │ %-19s │ %-12s │ %-11s │\n",
			marker, snapshotIDStr, timestamp, operation, parentID, records)
	}

	fmt.Println("└────────────────────┴─────────────────────────┴─────────────────────┴──────────────┴─────────────┘")
	fmt.Println("* = current snapshot")

	return nil
}

func displayTableHistoryJSON(snapshots []table.Snapshot, reverse bool) error {
	// Display order
	displayOrder := snapshots
	if !reverse {
		// Reverse to show newest first (default)
		displayOrder = make([]table.Snapshot, len(snapshots))
		for i, snapshot := range snapshots {
			displayOrder[len(snapshots)-1-i] = snapshot
		}
	}

	fmt.Println("[")
	for i, snapshot := range displayOrder {
		timestamp := time.UnixMilli(snapshot.TimestampMs).Format(time.RFC3339)

		operation := "unknown"
		if snapshot.Summary != nil {
			operation = string(snapshot.Summary.Operation)
		}

		parentID := "null"
		if snapshot.ParentSnapshotID != nil {
			parentID = fmt.Sprintf("%d", *snapshot.ParentSnapshotID)
		}

		fmt.Printf(`  {`)
		fmt.Printf(`"snapshot_id": %d, `, snapshot.SnapshotID)
		fmt.Printf(`"timestamp": "%s", `, timestamp)
		fmt.Printf(`"operation": "%s", `, operation)
		fmt.Printf(`"parent_snapshot_id": %s`, parentID)
		fmt.Print(`}`)

		if i < len(displayOrder)-1 {
			fmt.Print(",")
		}
		fmt.Println()
	}
	fmt.Println("]")

	return nil
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

	// Log what was applied
	if partitionSpec != nil && !partitionSpec.IsUnpartitioned() {
		var fieldCount int
		for range partitionSpec.Fields() {
			fieldCount++
		}
		fmt.Printf("✅ Applied partition specification with %d field(s)\n", fieldCount)
	}
	if sortOrder != nil && len(sortOrder.Fields) > 0 {
		fmt.Printf("✅ Applied sort order with %d field(s)\n", len(sortOrder.Fields))
	}

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
