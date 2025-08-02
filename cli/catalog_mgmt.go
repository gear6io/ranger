package cli

import (
	"fmt"
	"strings"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/display"
	"github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/spf13/cobra"
)

var catalogMgmtCmd = &cobra.Command{
	Use:   "catalog",
	Short: "Manage catalog namespaces",
	Long: `Manage namespaces within your Iceberg catalog.

This command provides subcommands for catalog operations:
- list: List all namespaces in the catalog
- create: Create a new namespace
- drop: Drop an existing namespace (if empty)

Examples:
  icebox catalog list                        # List all namespaces
  icebox catalog create analytics            # Create 'analytics' namespace  
  icebox catalog create warehouse.inventory  # Create nested namespace
  icebox catalog drop test_namespace         # Drop empty namespace`,
}

var catalogListCmd = &cobra.Command{
	Use:   "list",
	Short: "List all namespaces in the catalog",
	Long: `List all namespaces available in the catalog.

Shows both top-level and nested namespaces with their properties.

Examples:
  icebox catalog list                    # List all namespaces
  icebox catalog list --format json     # JSON output
  icebox catalog list --show-properties # Include namespace properties`,
	RunE: runCatalogList,
}

var catalogCreateCmd = &cobra.Command{
	Use:   "create <namespace>",
	Short: "Create a new namespace",
	Long: `Create a new namespace in the catalog.

Namespaces can be nested using dot notation. Parent namespaces
will be created automatically if they don't exist.

Examples:
  icebox catalog create analytics        # Create top-level namespace
  icebox catalog create warehouse.raw    # Create nested namespace
  icebox catalog create finance.reports.monthly  # Create deeply nested namespace`,
	Args: cobra.ExactArgs(1),
	RunE: runCatalogCreate,
}

var catalogDropCmd = &cobra.Command{
	Use:   "drop <namespace>",
	Short: "Drop an existing namespace",
	Long: `Drop an existing namespace from the catalog.

The namespace must be empty (contain no tables) to be dropped.
Use --force to drop non-empty namespaces (this will also drop all tables).

Examples:
  icebox catalog drop test_namespace     # Drop empty namespace
  icebox catalog drop old_data --force  # Force drop with all tables`,
	Args: cobra.ExactArgs(1),
	RunE: runCatalogDrop,
}

type catalogListOptions struct {
	format         string
	showProperties bool
	parent         string
}

type catalogCreateOptions struct {
	properties map[string]string
	location   string
}

type catalogDropOptions struct {
	force bool
}

var (
	catalogListOpts   = &catalogListOptions{}
	catalogCreateOpts = &catalogCreateOptions{}
	catalogDropOpts   = &catalogDropOptions{}
)

func init() {
	rootCmd.AddCommand(catalogMgmtCmd)

	// Add subcommands
	catalogMgmtCmd.AddCommand(catalogListCmd)
	catalogMgmtCmd.AddCommand(catalogCreateCmd)
	catalogMgmtCmd.AddCommand(catalogDropCmd)

	// Catalog list flags
	catalogListCmd.Flags().StringVar(&catalogListOpts.format, "format", "table", "output format: table, csv, json")
	catalogListCmd.Flags().BoolVar(&catalogListOpts.showProperties, "show-properties", false, "show namespace properties")
	catalogListCmd.Flags().StringVar(&catalogListOpts.parent, "parent", "", "list namespaces under specific parent")

	// Catalog create flags
	catalogCreateCmd.Flags().StringToStringVar(&catalogCreateOpts.properties, "property", nil, "namespace properties (key=value)")
	catalogCreateCmd.Flags().StringVar(&catalogCreateOpts.location, "location", "", "namespace location (optional)")

	// Catalog drop flags
	catalogDropCmd.Flags().BoolVar(&catalogDropOpts.force, "force", false, "force drop non-empty namespace")
}

func runCatalogList(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "catalog-list").Str("parent", catalogListOpts.parent).Msg("Starting catalog list operation")
	}

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-list").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		d.Info("Using configuration: %s", configPath)
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-list").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	// Determine parent namespace
	var parent table.Identifier
	if catalogListOpts.parent != "" {
		parent = strings.Split(catalogListOpts.parent, ".")
	}

	// List namespaces
	namespaces, err := cat.ListNamespaces(ctx, parent)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-list").Err(err).Msg("Failed to list namespaces")
		}
		d.Error("Failed to list namespaces: %v", err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "catalog-list").Int("namespace_count", len(namespaces)).Msg("Successfully listed namespaces")
	}

	// Display results
	if err := displayNamespaceListWithDisplay(namespaces, parent, d); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-list").Err(err).Msg("Failed to display namespace list")
		}
		d.Error("Failed to display namespace list: %v", err)
		return err
	}

	return nil
}

func runCatalogCreate(cmd *cobra.Command, args []string) error {
	namespaceName := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "catalog-create").Str("namespace", namespaceName).Msg("Starting namespace creation")
	}

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-create").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-create").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	// Parse namespace identifier
	namespace := strings.Split(namespaceName, ".")

	// Check if namespace already exists
	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-create").Str("namespace", namespaceName).Err(err).Msg("Failed to check namespace existence")
		}
		d.Error("Failed to check namespace existence: %v", err)
		return err
	}
	if exists {
		if logger != nil {
			logger.Warn().Str("cmd", "catalog-create").Str("namespace", namespaceName).Msg("Namespace already exists")
		}
		d.Error("Namespace '%s' already exists", namespaceName)
		return fmt.Errorf("namespace '%s' already exists", namespaceName)
	}

	// Prepare properties
	properties := iceberg.Properties{}
	for key, value := range catalogCreateOpts.properties {
		properties[key] = value
	}

	if catalogCreateOpts.location != "" {
		properties["location"] = catalogCreateOpts.location
	}

	// Create the namespace
	if err := cat.CreateNamespace(ctx, namespace, properties); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-create").Str("namespace", namespaceName).Err(err).Msg("Failed to create namespace")
		}
		d.Error("Failed to create namespace: %v", err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "catalog-create").Str("namespace", namespaceName).Int("property_count", len(properties)).Msg("Successfully created namespace")
	}

	// Display success message
	d.Success("Successfully created namespace!")
	d.Info("Namespace Details:")
	d.Info("   Name: %s", namespaceName)
	if len(properties) > 0 {
		d.Info("   Properties:")
		for key, value := range properties {
			d.Info("     %s: %s", key, value)
		}
	}

	return nil
}

func runCatalogDrop(cmd *cobra.Command, args []string) error {
	namespaceName := args[0]
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "catalog-drop").Str("namespace", namespaceName).Bool("force", catalogDropOpts.force).Msg("Starting namespace drop")
	}

	// Find the Icebox configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-drop").Err(err).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration: %v", err)
		d.Info("Try running 'icebox init' first to create a new project")
		return err
	}

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-drop").Err(err).Msg("Failed to create catalog")
		}
		d.Error("Failed to create catalog: %v", err)
		return err
	}
	defer cat.Close()

	// Parse namespace identifier
	namespace := strings.Split(namespaceName, ".")

	// Check if namespace exists
	exists, err := cat.CheckNamespaceExists(ctx, namespace)
	if err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-drop").Str("namespace", namespaceName).Err(err).Msg("Failed to check namespace existence")
		}
		d.Error("Failed to check namespace existence: %v", err)
		return err
	}
	if !exists {
		if logger != nil {
			logger.Warn().Str("cmd", "catalog-drop").Str("namespace", namespaceName).Msg("Namespace does not exist")
		}
		d.Error("Namespace '%s' does not exist", namespaceName)
		return fmt.Errorf("namespace '%s' does not exist", namespaceName)
	}

	// Check if namespace is empty (unless force is used)
	if !catalogDropOpts.force {
		var tables []table.Identifier
		for identifier, err := range cat.ListTables(ctx, namespace) {
			if err != nil {
				if logger != nil {
					logger.Error().Str("cmd", "catalog-drop").Str("namespace", namespaceName).Err(err).Msg("Failed to check if namespace is empty")
				}
				d.Error("Failed to check if namespace is empty: %v", err)
				return err
			}
			tables = append(tables, identifier)
		}

		if len(tables) > 0 {
			if logger != nil {
				logger.Warn().Str("cmd", "catalog-drop").Str("namespace", namespaceName).Int("table_count", len(tables)).Msg("Namespace is not empty")
			}
			d.Error("Namespace '%s' is not empty (contains %d tables)", namespaceName, len(tables))
			d.Info("Use --force to drop non-empty namespace or remove tables first")
			return fmt.Errorf("namespace '%s' is not empty (contains %d tables)", namespaceName, len(tables))
		}
	}

	// Drop the namespace
	if err := cat.DropNamespace(ctx, namespace); err != nil {
		if logger != nil {
			logger.Error().Str("cmd", "catalog-drop").Str("namespace", namespaceName).Err(err).Msg("Failed to drop namespace")
		}
		d.Error("Failed to drop namespace: %v", err)
		return err
	}

	if logger != nil {
		logger.Info().Str("cmd", "catalog-drop").Str("namespace", namespaceName).Msg("Successfully dropped namespace")
	}

	// Display success message
	d.Success("Successfully dropped namespace '%s'", namespaceName)

	return nil
}

// Helper functions for catalog management

// displayNamespaceListWithDisplay displays namespace list using the display package
func displayNamespaceListWithDisplay(namespaces []table.Identifier, parent table.Identifier, d display.Display) error {
	if len(namespaces) == 0 {
		if parent != nil {
			d.Info("No namespaces found under '%s'", strings.Join(parent, "."))
		} else {
			d.Info("No namespaces found")
		}
		return nil
	}

	switch catalogListOpts.format {
	case "table":
		return displayNamespaceListTableWithDisplay(namespaces, parent, d)
	case "csv":
		return displayNamespaceListCSVWithDisplay(namespaces, d)
	case "json":
		return displayNamespaceListJSONWithDisplay(namespaces, d)
	default:
		d.Error("Unsupported format: %s", catalogListOpts.format)
		return fmt.Errorf("unsupported format: %s", catalogListOpts.format)
	}
}

// displayNamespaceListTableWithDisplay displays namespace list in table format using display package
func displayNamespaceListTableWithDisplay(namespaces []table.Identifier, parent table.Identifier, d display.Display) error {
	// Prepare table data
	headers := []string{"#", "Namespace", "Level"}
	var rows [][]interface{}

	for i, namespace := range namespaces {
		namespaceName := strings.Join(namespace, ".")
		level := len(namespace)
		rows = append(rows, []interface{}{i + 1, namespaceName, level})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	title := fmt.Sprintf("All Namespaces (%d namespaces)", len(namespaces))
	if parent != nil {
		title = fmt.Sprintf("Namespaces under '%s' (%d namespaces)", strings.Join(parent, "."), len(namespaces))
	}

	return d.Table(tableData).WithTitle(title).Render()
}

// displayNamespaceListCSVWithDisplay displays namespace list in CSV format using display package
func displayNamespaceListCSVWithDisplay(namespaces []table.Identifier, d display.Display) error {
	// Prepare table data
	headers := []string{"namespace", "level"}
	var rows [][]interface{}

	for _, namespace := range namespaces {
		namespaceName := strings.Join(namespace, ".")
		level := len(namespace)
		rows = append(rows, []interface{}{namespaceName, level})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	return d.Table(tableData).WithFormat(display.FormatCSV).Render()
}

// displayNamespaceListJSONWithDisplay displays namespace list in JSON format using display package
func displayNamespaceListJSONWithDisplay(namespaces []table.Identifier, d display.Display) error {
	// Prepare table data
	headers := []string{"namespace", "level"}
	var rows [][]interface{}

	for _, namespace := range namespaces {
		namespaceName := strings.Join(namespace, ".")
		level := len(namespace)
		rows = append(rows, []interface{}{namespaceName, level})
	}

	tableData := display.TableData{
		Headers: headers,
		Rows:    rows,
	}

	return d.Table(tableData).WithFormat(display.FormatJSON).Render()
}
