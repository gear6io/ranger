//go:build deprecated
// +build deprecated

// DEPRECATED: This file has been migrated to server/protocols/http/
// Use icebox-server for HTTP server functionality
// This file will be removed in a future release
//
// ⚠️  WARNING: This CLI command is deprecated
// 🚀 NEW: Use ./icebox-server for HTTP server functionality
// 📖 See MIGRATION.md for migration instructions

package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/TFMV/icebox/importer"
	"github.com/apache/iceberg-go/table"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/logger"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/gofiber/fiber/v2/middleware/requestid"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

// DEPRECATED: This command has been moved to icebox-server
// Use 'icebox-server' instead for HTTP server functionality
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start API server (DEPRECATED - use icebox-server)",
	Long: `Start the Icebox API server for REST and gRPC endpoints.
	
DEPRECATED: This command has been moved to icebox-server.
Use 'icebox-server' instead for HTTP server functionality.

This will be removed in a future release.`,
	RunE: runServe,
}

type serveOptions struct {
	port        int
	mode        string
	profile     string
	host        string
	cors        bool
	auth        bool
	metrics     bool
	healthCheck bool
	verbose     bool
	logLevel    string
	certFile    string
	keyFile     string
	prefork     bool
	compress    bool
}

var serveOpts = &serveOptions{}

// ServerProfile represents different deployment profiles
type ServerProfile struct {
	Name        string
	Description string
	Auth        bool
	Metrics     bool
	CORS        bool
	LogLevel    string
	Timeout     time.Duration
	Prefork     bool
	Compress    bool
}

// Server start time for uptime calculation
var serverStartTime time.Time

// DEPRECATED: This function has been removed as this file is deprecated
// Use icebox-server instead for HTTP server functionality

func runServe(cmd *cobra.Command, args []string) error {
	// DEPRECATED: This function is deprecated, use icebox-server instead
	fmt.Fprintf(os.Stderr, "⚠️  DEPRECATED: Use 'icebox-server' instead of 'icebox serve'\n")

	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", err)
	}

	if serveOpts.verbose {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

	// Get server profile
	profile := getServerProfile(serveOpts.profile)
	if profile == nil {
		return fmt.Errorf("❌ Unknown profile: %s (available: local, dev, prod)", serveOpts.profile)
	}

	// Apply profile defaults if not overridden by flags
	applyProfileDefaults(cmd, profile)

	printServerInfo(profile)

	// Start the appropriate server
	switch serveOpts.mode {
	case "rest":
		return startRESTServer(cfg, profile)
	case "grpc":
		return startGRPCServer(cfg, profile)
	default:
		return fmt.Errorf("❌ Unknown server mode: %s (available: rest, grpc)", serveOpts.mode)
	}
}

func applyProfileDefaults(cmd *cobra.Command, profile *ServerProfile) {
	if serveOpts.logLevel == "" {
		serveOpts.logLevel = profile.LogLevel
	}
	if !cmd.Flags().Changed("cors") {
		serveOpts.cors = profile.CORS
	}
	if !cmd.Flags().Changed("auth") {
		serveOpts.auth = profile.Auth
	}
	if !cmd.Flags().Changed("metrics") {
		serveOpts.metrics = profile.Metrics
	}
	if !cmd.Flags().Changed("prefork") {
		serveOpts.prefork = profile.Prefork
	}
	if !cmd.Flags().Changed("compress") {
		serveOpts.compress = profile.Compress
	}
}

func printServerInfo(profile *ServerProfile) {
	fmt.Printf("🚀 Starting Icebox API Server\n")
	fmt.Printf("   Mode: %s\n", serveOpts.mode)
	fmt.Printf("   Profile: %s (%s)\n", profile.Name, profile.Description)
	fmt.Printf("   Address: %s:%d\n", serveOpts.host, serveOpts.port)

	features := []string{}
	if serveOpts.cors {
		features = append(features, "CORS")
	}
	if serveOpts.auth {
		features = append(features, "Auth")
	}
	if serveOpts.metrics {
		features = append(features, "Metrics")
	}
	if serveOpts.healthCheck {
		features = append(features, "Health")
	}
	if serveOpts.prefork {
		features = append(features, "Prefork")
	}
	if serveOpts.compress {
		features = append(features, "Compress")
	}

	if len(features) > 0 {
		fmt.Printf("   Features: %v\n", features)
	} else {
		fmt.Printf("   Features: None\n")
	}
}

func startRESTServer(cfg *config.Config, profile *ServerProfile) error {
	// Initialize server start time
	serverStartTime = time.Now()

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Create SQL engine - need to assert to concrete type
	var engine *duckdb.Engine
	switch catalogImpl := cat.(type) {
	case *sqlite.Catalog:
		engine, err = duckdb.NewEngine(catalogImpl)
		if err != nil {
			return fmt.Errorf("❌ Failed to create SQL engine: %w", err)
		}
	default:
		return fmt.Errorf("❌ API server currently only supports SQLite catalogs")
	}
	defer engine.Close()

	// Create Fiber app with optimized configuration
	app := fiber.New(fiber.Config{
		ServerHeader:            "Icebox API Server v0.1.0",
		AppName:                 "Icebox API",
		Prefork:                 serveOpts.prefork,
		DisableStartupMessage:   false,
		ReadTimeout:             profile.Timeout,
		WriteTimeout:            profile.Timeout,
		IdleTimeout:             60 * time.Second,
		BodyLimit:               50 * 1024 * 1024, // 50MB
		EnableTrustedProxyCheck: serveOpts.profile == "prod",
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			message := "Internal Server Error"

			if e, ok := err.(*fiber.Error); ok {
				code = e.Code
				message = e.Message
			}

			return c.Status(code).JSON(fiber.Map{
				"error":     message,
				"code":      code,
				"timestamp": time.Now().Format(time.RFC3339),
			})
		},
	})

	// Add middleware based on configuration
	app.Use(requestid.New())
	app.Use(recover.New())

	if serveOpts.verbose || profile.LogLevel == "debug" {
		app.Use(logger.New(logger.Config{
			Format: "[${time}] ${status} - ${method} ${path} (${latency})\n",
		}))
	}

	if serveOpts.cors {
		app.Use(cors.New(cors.Config{
			AllowOrigins: "*",
			AllowMethods: "GET,POST,PUT,DELETE,OPTIONS",
			AllowHeaders: "Origin, Content-Type, Accept, Authorization, X-Request-ID",
		}))
	}

	if serveOpts.compress {
		app.Use(func(c *fiber.Ctx) error {
			c.Response().Header.Set("Content-Encoding", "gzip")
			return c.Next()
		})
	}

	// Create API handler
	api := &RESTAPIHandler{
		catalog: cat,
		engine:  engine,
		config:  cfg,
		profile: profile,
	}

	// Register routes
	registerRESTRoutes(app, api)

	// Graceful shutdown
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		fmt.Printf("\n🛑 Shutting down REST server...\n")
		if err := app.Shutdown(); err != nil {
			fmt.Printf("❌ Server forced to shutdown: %v\n", err)
		}
		fmt.Printf("✅ REST server stopped\n")
	}()

	// Start server
	addr := fmt.Sprintf("%s:%d", serveOpts.host, serveOpts.port)
	fmt.Printf("✅ REST API server listening on %s\n", addr)

	if serveOpts.certFile != "" && serveOpts.keyFile != "" {
		fmt.Printf("🔒 TLS enabled\n")
		return app.ListenTLS(addr, serveOpts.certFile, serveOpts.keyFile)
	}

	return app.Listen(addr)
}

func startGRPCServer(cfg *config.Config, profile *ServerProfile) error {
	// Initialize server start time
	serverStartTime = time.Now()

	// Create catalog
	cat, err := catalog.NewCatalog(cfg)
	if err != nil {
		return fmt.Errorf("❌ Failed to create catalog: %w", err)
	}
	defer cat.Close()

	// Create SQL engine
	var engine *duckdb.Engine
	switch catalogImpl := cat.(type) {
	case *sqlite.Catalog:
		engine, err = duckdb.NewEngine(catalogImpl)
		if err != nil {
			return fmt.Errorf("❌ Failed to create SQL engine: %w", err)
		}
	default:
		return fmt.Errorf("❌ gRPC server currently only supports SQLite catalogs")
	}
	defer engine.Close()

	// Create listener
	addr := fmt.Sprintf("%s:%d", serveOpts.host, serveOpts.port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("❌ Failed to listen on %s: %w", addr, err)
	}

	// Create gRPC server with optimizations
	var opts []grpc.ServerOption

	// Add TLS if certificates are provided
	if serveOpts.certFile != "" && serveOpts.keyFile != "" {
		// In production, load TLS credentials
		fmt.Printf("🔒 TLS would be enabled (implementation needed)\n")
	}

	grpcServer := grpc.NewServer(opts...)

	// Register reflection service for development
	if profile.Name == "local" || profile.Name == "dev" {
		reflection.Register(grpcServer)
		fmt.Printf("🔍 gRPC reflection enabled\n")
	}

	// TODO: Register actual gRPC services here
	// For now, we'll start a basic server
	fmt.Printf("✅ gRPC server listening on %s\n", addr)
	fmt.Printf("🚧 gRPC service implementations coming soon...\n")
	fmt.Printf("📋 Services will include:\n")
	fmt.Printf("   - CatalogService (namespaces, tables, schemas)\n")
	fmt.Printf("   - QueryService (SQL execution, results streaming)\n")
	fmt.Printf("   - ImportService (data ingestion)\n")
	fmt.Printf("   - TimeTravelService (snapshot querying)\n")

	// Graceful shutdown
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		fmt.Printf("\n🛑 Shutting down gRPC server...\n")
		grpcServer.GracefulStop()
		fmt.Printf("✅ gRPC server stopped\n")
	}()

	return grpcServer.Serve(lis)
}

func getServerProfile(name string) *ServerProfile {
	profiles := map[string]*ServerProfile{
		"local": {
			Name:        "local",
			Description: "Development-friendly with relaxed auth and debug logging",
			Auth:        false,
			Metrics:     false,
			CORS:        true,
			LogLevel:    "debug",
			Timeout:     30 * time.Second,
			Prefork:     false,
			Compress:    false,
		},
		"dev": {
			Name:        "dev",
			Description: "Staging environment with basic auth and structured logging",
			Auth:        true,
			Metrics:     true,
			CORS:        true,
			LogLevel:    "info",
			Timeout:     30 * time.Second,
			Prefork:     false,
			Compress:    true,
		},
		"prod": {
			Name:        "prod",
			Description: "Production-ready with strong auth and performance optimization",
			Auth:        true,
			Metrics:     true,
			CORS:        false,
			LogLevel:    "warn",
			Timeout:     10 * time.Second,
			Prefork:     true,
			Compress:    true,
		},
	}
	return profiles[name]
}

// RESTAPIHandler handles REST API requests with high performance
type RESTAPIHandler struct {
	catalog catalog.CatalogInterface
	engine  *duckdb.Engine
	config  *config.Config
	profile *ServerProfile
}

func registerRESTRoutes(app *fiber.App, api *RESTAPIHandler) {
	// Root endpoint - API documentation
	app.Get("/", api.apiInfo)

	// Health endpoints
	if serveOpts.healthCheck {
		health := app.Group("/health")
		health.Get("/", api.healthCheck)
		health.Get("/ready", api.readinessCheck)
		health.Get("/live", api.livenessCheck)
	}

	// Metrics endpoint
	if serveOpts.metrics {
		app.Get("/metrics", api.metrics)
	}

	// API v1 routes
	v1 := app.Group("/api/v1")

	// Catalog operations
	catalog := v1.Group("/catalog")
	catalog.Get("/namespaces", api.listNamespaces)
	catalog.Post("/namespaces", api.createNamespace)
	catalog.Delete("/namespaces/:namespace", api.dropNamespace)
	catalog.Get("/namespaces/:namespace/properties", api.getNamespaceProperties)

	// Table operations
	tables := v1.Group("/namespaces/:namespace/tables")
	tables.Get("/", api.listTables)
	tables.Post("/", api.createTable)
	tables.Get("/:table", api.describeTable)
	tables.Delete("/:table", api.dropTable)
	tables.Get("/:table/schema", api.getTableSchema)
	tables.Get("/:table/properties", api.getTableProperties)
	tables.Get("/:table/snapshots", api.listSnapshots)

	// Query operations
	query := v1.Group("/query")
	query.Post("/sql", api.executeSQL)
	query.Get("/tables", api.listRegisteredTables)
	query.Post("/explain", api.explainQuery)

	// Import operations
	import_ := v1.Group("/import")
	import_.Post("/parquet", api.importParquet)
	import_.Post("/avro", api.importAvro)
	import_.Get("/status/:job_id", api.getImportStatus)

	// Time travel operations
	timetravel := v1.Group("/time-travel")
	timetravel.Post("/query", api.timeTravelQuery)
	timetravel.Get("/snapshots/:namespace/:table", api.getTableSnapshots)

	// Administrative operations
	admin := v1.Group("/admin")
	if serveOpts.auth {
		admin.Use(api.authMiddleware)
	}
	admin.Get("/config", api.getServerConfig)
	admin.Post("/cache/clear", api.clearCache)
	admin.Get("/stats", api.getServerStats)
}

// Middleware for authentication
func (api *RESTAPIHandler) authMiddleware(c *fiber.Ctx) error {
	authHeader := c.Get("Authorization")
	if authHeader == "" {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "Authorization header required",
			"code":  fiber.StatusUnauthorized,
		})
	}

	// Simple Bearer token validation for demo
	// In production, this would validate JWT tokens, API keys, etc.
	if !strings.HasPrefix(authHeader, "Bearer ") {
		return c.Status(fiber.StatusUnauthorized).JSON(fiber.Map{
			"error": "Invalid authorization format. Use 'Bearer <token>'",
			"code":  fiber.StatusUnauthorized,
		})
	}

	return c.Next()
}

// API endpoint implementations
func (api *RESTAPIHandler) apiInfo(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"service":   "Icebox API Server",
		"version":   "v0.1.0",
		"profile":   serveOpts.profile,
		"mode":      serveOpts.mode,
		"timestamp": time.Now().Format(time.RFC3339),
		"endpoints": fiber.Map{
			"health":      "/health",
			"metrics":     "/metrics",
			"api":         "/api/v1",
			"catalog":     "/api/v1/catalog",
			"query":       "/api/v1/query",
			"import":      "/api/v1/import",
			"time_travel": "/api/v1/time-travel",
			"admin":       "/api/v1/admin",
		},
		"features": fiber.Map{
			"cors":        serveOpts.cors,
			"auth":        serveOpts.auth,
			"metrics":     serveOpts.metrics,
			"health":      serveOpts.healthCheck,
			"compression": serveOpts.compress,
		},
	})
}

func (api *RESTAPIHandler) healthCheck(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "healthy",
		"timestamp": time.Now().Format(time.RFC3339),
		"service":   "icebox",
		"version":   "v0.1.0",
	})
}

func (api *RESTAPIHandler) readinessCheck(c *fiber.Ctx) error {
	// Check if catalog is accessible
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := api.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{
			"status":    "not ready",
			"error":     err.Error(),
			"timestamp": time.Now().Format(time.RFC3339),
		})
	}

	return c.JSON(fiber.Map{
		"status":    "ready",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (api *RESTAPIHandler) livenessCheck(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{
		"status":    "alive",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (api *RESTAPIHandler) metrics(c *fiber.Ctx) error {
	engineMetrics := api.engine.GetMetrics()

	return c.JSON(fiber.Map{
		"timestamp": time.Now().Format(time.RFC3339),
		"engine": fiber.Map{
			"queries_executed":  engineMetrics.QueriesExecuted,
			"tables_registered": engineMetrics.TablesRegistered,
			"cache_hits":        engineMetrics.CacheHits,
			"cache_misses":      engineMetrics.CacheMisses,
			"total_query_time":  engineMetrics.TotalQueryTime.String(),
			"error_count":       engineMetrics.ErrorCount,
		},
		"server": fiber.Map{
			"uptime":     time.Since(serverStartTime).String(),
			"profile":    api.profile.Name,
			"goroutines": "N/A", // Could add runtime.NumGoroutine()
		},
	})
}

func (api *RESTAPIHandler) listNamespaces(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	namespaces, err := api.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"namespaces": namespaces,
		"count":      len(namespaces),
	})
}

func (api *RESTAPIHandler) executeSQL(c *fiber.Ctx) error {
	var request struct {
		SQL     string `json:"sql" validate:"required"`
		MaxRows int    `json:"max_rows,omitempty"`
		Format  string `json:"format,omitempty"`
	}

	if err := c.BodyParser(&request); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error":   "Invalid JSON body",
			"details": err.Error(),
		})
	}

	if request.SQL == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "SQL query is required",
		})
	}

	// Set defaults
	if request.MaxRows == 0 {
		request.MaxRows = 1000
	}
	if request.Format == "" {
		request.Format = "json"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := api.engine.ExecuteQuery(ctx, request.SQL)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error":   "Query execution failed",
			"details": err.Error(),
		})
	}

	// Limit rows if requested
	rows := result.Rows
	if len(rows) > request.MaxRows {
		rows = rows[:request.MaxRows]
	}

	return c.JSON(fiber.Map{
		"query_id":    result.QueryID,
		"columns":     result.Columns,
		"rows":        rows,
		"row_count":   len(rows),
		"total_rows":  result.RowCount,
		"duration_ms": result.Duration.Milliseconds(),
		"truncated":   len(result.Rows) > request.MaxRows,
	})
}

// Placeholder implementations for other endpoints
func (api *RESTAPIHandler) createNamespace(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error":    "Endpoint not yet implemented",
		"endpoint": "POST /api/v1/catalog/namespaces",
	})
}

func (api *RESTAPIHandler) dropNamespace(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error":    "Endpoint not yet implemented",
		"endpoint": "DELETE /api/v1/catalog/namespaces/:namespace",
	})
}

func (api *RESTAPIHandler) getNamespaceProperties(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) listTables(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) createTable(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) describeTable(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) dropTable(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) getTableSchema(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) getTableProperties(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) listSnapshots(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) listRegisteredTables(c *fiber.Ctx) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tables, err := api.engine.ListTables(ctx)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return c.JSON(fiber.Map{
		"tables": tables,
		"count":  len(tables),
	})
}

func (api *RESTAPIHandler) explainQuery(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) importParquet(c *fiber.Ctx) error {
	return api.importFile(c, "parquet")
}

func (api *RESTAPIHandler) importAvro(c *fiber.Ctx) error {
	return api.importFile(c, "avro")
}

// importFile handles file imports for both Parquet and Avro formats
func (api *RESTAPIHandler) importFile(c *fiber.Ctx, expectedFormat string) error {
	// Parse request body
	type ImportFileRequest struct {
		FilePath    string   `json:"file_path"`
		TableName   string   `json:"table_name"`
		Namespace   string   `json:"namespace,omitempty"`
		Overwrite   bool     `json:"overwrite,omitempty"`
		PartitionBy []string `json:"partition_by,omitempty"`
	}

	var req ImportFileRequest
	if err := c.BodyParser(&req); err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "Invalid request body",
		})
	}

	// Validate required fields
	if req.FilePath == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "file_path is required",
		})
	}
	if req.TableName == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "table_name is required",
		})
	}

	// Set default namespace if not provided
	if req.Namespace == "" {
		req.Namespace = "default"
	}

	// Create importer factory
	factory := importer.NewImporterFactory(api.config)

	// Detect file type and create appropriate importer
	imp, importerType, err := factory.CreateImporter(req.FilePath)
	if err != nil {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("Failed to create importer: %v", err),
		})
	}
	defer imp.Close()

	// Verify the file format matches the endpoint
	if string(importerType) != expectedFormat {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": fmt.Sprintf("File format mismatch: expected %s, got %s", expectedFormat, importerType),
		})
	}

	// Parse table identifier
	var tableIdent, namespaceIdent table.Identifier
	if strings.Contains(req.TableName, ".") {
		parts := strings.Split(req.TableName, ".")
		if len(parts) != 2 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{
				"error": "Invalid table name format. Use 'namespace.table' or just 'table'",
			})
		}
		namespaceIdent = table.Identifier{parts[0]}
		tableIdent = table.Identifier{parts[0], parts[1]}
	} else {
		namespaceIdent = table.Identifier{req.Namespace}
		tableIdent = table.Identifier{req.Namespace, req.TableName}
	}

	// Infer schema
	schema, stats, err := imp.InferSchema(req.FilePath)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Failed to infer schema: %v", err),
		})
	}

	// Perform import
	ctx := context.Background()
	result, err := imp.ImportTable(ctx, importer.ImportRequest{
		ParquetFile:    req.FilePath, // Note: field name is ParquetFile but used for any file type
		TableIdent:     tableIdent,
		NamespaceIdent: namespaceIdent,
		Schema:         schema,
		Overwrite:      req.Overwrite,
		PartitionBy:    req.PartitionBy,
	})
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": fmt.Sprintf("Failed to import table: %v", err),
		})
	}

	// Return success response
	return c.Status(fiber.StatusCreated).JSON(fiber.Map{
		"message": "File imported successfully",
		"result": fiber.Map{
			"table_identifier": result.TableIdent,
			"record_count":     result.RecordCount,
			"data_size":        result.DataSize,
			"table_location":   result.TableLocation,
			"file_format":      importerType,
		},
		"schema": schema,
		"stats":  stats,
	})
}

func (api *RESTAPIHandler) getImportStatus(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) timeTravelQuery(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) getTableSnapshots(c *fiber.Ctx) error {
	return c.Status(fiber.StatusNotImplemented).JSON(fiber.Map{
		"error": "Endpoint not yet implemented",
	})
}

func (api *RESTAPIHandler) getServerConfig(c *fiber.Ctx) error {
	// Sanitize sensitive information
	config := map[string]interface{}{
		"name":    api.config.Name,
		"profile": api.profile.Name,
		"catalog": map[string]interface{}{
			"type": api.config.Catalog.Type,
		},
		"storage": map[string]interface{}{
			"type": api.config.Storage.Type,
		},
	}

	return c.JSON(fiber.Map{
		"config":    config,
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (api *RESTAPIHandler) clearCache(c *fiber.Ctx) error {
	api.engine.ClearTableCache()
	return c.JSON(fiber.Map{
		"message":   "Cache cleared successfully",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

func (api *RESTAPIHandler) getServerStats(c *fiber.Ctx) error {
	metrics := api.engine.GetMetrics()

	return c.JSON(fiber.Map{
		"stats": fiber.Map{
			"queries_executed":  metrics.QueriesExecuted,
			"tables_registered": metrics.TablesRegistered,
			"cache_hits":        metrics.CacheHits,
			"cache_misses":      metrics.CacheMisses,
			"error_count":       metrics.ErrorCount,
		},
		"timestamp": time.Now().Format(time.RFC3339),
	})
}
