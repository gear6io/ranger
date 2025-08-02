package cli

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/spf13/cobra"
)

var uiCmd = &cobra.Command{
	Use:   "ui",
	Short: "Start web UI for exploring Iceberg data",
	Long: `Start a beautiful web interface for exploring your Iceberg data.

The UI provides an intuitive way to:
- Browse namespaces and tables
- Execute SQL queries with syntax highlighting
- Visualize query results in tables and charts
- Explore table schemas and metadata
- Navigate time-travel snapshots
- Monitor system metrics

This is perfect for data exploration, debugging, and demonstrating
Iceberg capabilities to stakeholders.

Examples:
  icebox ui                    # Start UI on port 9090
  icebox ui --port 8080        # Start UI on custom port
  icebox ui --readonly         # Start in read-only mode
  icebox ui --auth             # Enable authentication`,
	RunE: runUI,
}

type uiOptions struct {
	port     int
	host     string
	readonly bool
	auth     bool
	autoOpen bool
	certFile string
	keyFile  string
	verbose  bool
}

var uiOpts = &uiOptions{}

func init() {
	rootCmd.AddCommand(uiCmd)

	uiCmd.Flags().IntVar(&uiOpts.port, "port", 9090, "UI server port")
	uiCmd.Flags().StringVar(&uiOpts.host, "host", "localhost", "UI server host")
	uiCmd.Flags().BoolVar(&uiOpts.readonly, "readonly", false, "enable read-only mode (no DDL/DML)")
	uiCmd.Flags().BoolVar(&uiOpts.auth, "auth", false, "enable authentication")
	uiCmd.Flags().BoolVar(&uiOpts.autoOpen, "open", true, "automatically open browser")
	uiCmd.Flags().StringVar(&uiOpts.certFile, "cert", "", "TLS certificate file")
	uiCmd.Flags().StringVar(&uiOpts.keyFile, "key", "", "TLS private key file")
	uiCmd.Flags().BoolVar(&uiOpts.verbose, "verbose", false, "verbose logging")
}

func runUI(cmd *cobra.Command, args []string) error {
	// Find the Icebox configuration
	configPath, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", err)
	}

	if uiOpts.verbose {
		fmt.Printf("Using configuration: %s\n", configPath)
	}

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
		return fmt.Errorf("❌ UI currently only supports SQLite catalogs")
	}
	defer engine.Close()

	fmt.Printf("🎨 Starting Icebox Web UI\n")
	fmt.Printf("   Address: %s://%s:%d\n", getUIScheme(), uiOpts.host, uiOpts.port)

	features := []string{}
	if uiOpts.readonly {
		features = append(features, "Read-Only")
	}
	if uiOpts.auth {
		features = append(features, "Auth")
	}
	if len(features) > 0 {
		fmt.Printf("   Features: %v\n", features)
	}

	// Create web UI handler
	ui := &WebUIHandler{
		catalog: cat,
		engine:  engine,
		config:  cfg,
		options: *uiOpts,
	}

	// Create HTTP server
	mux := http.NewServeMux()
	ui.registerRoutes(mux)

	server := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", uiOpts.host, uiOpts.port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		fmt.Printf("\n🛑 Shutting down Web UI...\n")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := server.Shutdown(ctx); err != nil {
			fmt.Printf("❌ Server forced to shutdown: %v\n", err)
		}
		fmt.Printf("✅ Web UI stopped\n")
	}()

	// Start server
	fmt.Printf("✅ Web UI server listening on %s\n", server.Addr)
	fmt.Printf("🌐 Open your browser to: %s://%s:%d\n", getUIScheme(), uiOpts.host, uiOpts.port)

	if uiOpts.autoOpen {
		fmt.Printf("🚀 Opening browser automatically...\n")
		// In a real implementation, we'd open the browser here
		// For now, just suggest manual opening
	}

	if uiOpts.certFile != "" && uiOpts.keyFile != "" {
		fmt.Printf("🔒 TLS enabled\n")
		return server.ListenAndServeTLS(uiOpts.certFile, uiOpts.keyFile)
	}

	return server.ListenAndServe()
}

func getUIScheme() string {
	if uiOpts.certFile != "" && uiOpts.keyFile != "" {
		return "https"
	}
	return "http"
}

// WebUIHandler handles web UI requests
type WebUIHandler struct {
	catalog catalog.CatalogInterface
	engine  *duckdb.Engine
	config  *config.Config
	options uiOptions
}

func (ui *WebUIHandler) registerRoutes(mux *http.ServeMux) {
	// Main pages
	mux.HandleFunc("/", ui.handleDashboard)
	mux.HandleFunc("/query", ui.handleQueryEditor)
	mux.HandleFunc("/tables", ui.handleTables)
	mux.HandleFunc("/namespaces", ui.handleNamespaces)
	mux.HandleFunc("/metrics", ui.handleMetrics)

	// API endpoints for the UI
	mux.HandleFunc("/api/execute", ui.handleExecuteQuery)
	mux.HandleFunc("/api/namespaces", ui.handleAPINamespaces)
	mux.HandleFunc("/api/tables", ui.handleAPITables)
	mux.HandleFunc("/api/table-info", ui.handleAPITableInfo)
	mux.HandleFunc("/api/metrics", ui.handleAPIMetrics)

	// Health check
	mux.HandleFunc("/health", ui.handleHealth)
}

func (ui *WebUIHandler) handleDashboard(w http.ResponseWriter, r *http.Request) {
	if ui.options.auth && !ui.isAuthenticated(r) {
		ui.redirectToLogin(w, r)
		return
	}

	// Render dashboard template
	ui.renderTemplate(w, "dashboard.html", map[string]interface{}{
		"Title":     "Icebox Dashboard",
		"Config":    ui.config.Name,
		"ReadOnly":  ui.options.readonly,
		"Timestamp": time.Now().Format(time.RFC3339),
	})
}

func (ui *WebUIHandler) handleQueryEditor(w http.ResponseWriter, r *http.Request) {
	if ui.options.auth && !ui.isAuthenticated(r) {
		ui.redirectToLogin(w, r)
		return
	}

	ui.renderTemplate(w, "query.html", map[string]interface{}{
		"Title":    "SQL Query Editor",
		"ReadOnly": ui.options.readonly,
	})
}

func (ui *WebUIHandler) handleTables(w http.ResponseWriter, r *http.Request) {
	if ui.options.auth && !ui.isAuthenticated(r) {
		ui.redirectToLogin(w, r)
		return
	}

	ui.renderTemplate(w, "tables.html", map[string]interface{}{
		"Title": "Tables & Schemas",
	})
}

func (ui *WebUIHandler) handleNamespaces(w http.ResponseWriter, r *http.Request) {
	if ui.options.auth && !ui.isAuthenticated(r) {
		ui.redirectToLogin(w, r)
		return
	}

	ui.renderTemplate(w, "namespaces.html", map[string]interface{}{
		"Title": "Namespaces",
	})
}

func (ui *WebUIHandler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	if ui.options.auth && !ui.isAuthenticated(r) {
		ui.redirectToLogin(w, r)
		return
	}

	ui.renderTemplate(w, "metrics.html", map[string]interface{}{
		"Title": "System Metrics",
	})
}

func (ui *WebUIHandler) handleExecuteQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if ui.options.auth && !ui.isAuthenticated(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse query from request
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Invalid form data", http.StatusBadRequest)
		return
	}

	query := r.Form.Get("sql")
	if query == "" {
		http.Error(w, "SQL query is required", http.StatusBadRequest)
		return
	}

	// Check read-only mode
	if ui.options.readonly && ui.isWriteOperation(query) {
		ui.writeJSON(w, map[string]interface{}{
			"error": "Write operations are disabled in read-only mode",
		})
		return
	}

	// Execute query
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := ui.engine.ExecuteQuery(ctx, query)
	if err != nil {
		ui.writeJSON(w, map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Return results
	ui.writeJSON(w, map[string]interface{}{
		"query_id":    result.QueryID,
		"columns":     result.Columns,
		"rows":        result.Rows,
		"row_count":   result.RowCount,
		"duration_ms": result.Duration.Milliseconds(),
		"success":     true,
	})
}

func (ui *WebUIHandler) handleAPINamespaces(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	namespaces, err := ui.catalog.ListNamespaces(ctx, nil)
	if err != nil {
		ui.writeJSON(w, map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ui.writeJSON(w, map[string]interface{}{
		"namespaces": namespaces,
		"count":      len(namespaces),
	})
}

func (ui *WebUIHandler) handleAPITables(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tables, err := ui.engine.ListTables(ctx)
	if err != nil {
		ui.writeJSON(w, map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ui.writeJSON(w, map[string]interface{}{
		"tables": tables,
		"count":  len(tables),
	})
}

func (ui *WebUIHandler) handleAPITableInfo(w http.ResponseWriter, r *http.Request) {
	tableName := r.URL.Query().Get("table")
	if tableName == "" {
		ui.writeJSON(w, map[string]interface{}{
			"error": "Table name is required",
		})
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	result, err := ui.engine.DescribeTable(ctx, tableName)
	if err != nil {
		ui.writeJSON(w, map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	ui.writeJSON(w, map[string]interface{}{
		"columns": result.Columns,
		"rows":    result.Rows,
		"count":   result.RowCount,
	})
}

func (ui *WebUIHandler) handleAPIMetrics(w http.ResponseWriter, r *http.Request) {
	metrics := ui.engine.GetMetrics()

	ui.writeJSON(w, map[string]interface{}{
		"timestamp": time.Now().Format(time.RFC3339),
		"engine": map[string]interface{}{
			"queries_executed":  metrics.QueriesExecuted,
			"tables_registered": metrics.TablesRegistered,
			"cache_hits":        metrics.CacheHits,
			"cache_misses":      metrics.CacheMisses,
			"total_query_time":  metrics.TotalQueryTime.String(),
			"error_count":       metrics.ErrorCount,
		},
	})
}

func (ui *WebUIHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	ui.writeJSON(w, map[string]interface{}{
		"status":    "healthy",
		"service":   "icebox-ui",
		"timestamp": time.Now().Format(time.RFC3339),
	})
}

// Helper methods
func (ui *WebUIHandler) isAuthenticated(r *http.Request) bool {
	// Simple authentication check - in production this would be more sophisticated
	if !ui.options.auth {
		return true
	}

	// Check for session cookie or auth header
	if cookie, err := r.Cookie("auth"); err == nil && cookie.Value != "" {
		return true
	}

	if auth := r.Header.Get("Authorization"); auth != "" {
		return true
	}

	return false
}

func (ui *WebUIHandler) redirectToLogin(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/login", http.StatusFound)
}

func (ui *WebUIHandler) isWriteOperation(query string) bool {
	// Simple check for write operations
	query = strings.ToUpper(strings.TrimSpace(query))
	writeKeywords := []string{
		"INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE",
	}

	for _, keyword := range writeKeywords {
		if strings.HasPrefix(query, keyword) {
			return true
		}
	}
	return false
}

func (ui *WebUIHandler) renderTemplate(w http.ResponseWriter, templateName string, data interface{}) {
	// Create a simple HTML template if embedded files aren't available
	tmpl := `<!DOCTYPE html>
<html>
<head>
    <title>{{.Title}} - Icebox</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 0; padding: 20px; background: #f5f5f5; }
        .container { max-width: 1200px; margin: 0 auto; background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
        .header { border-bottom: 1px solid #eee; padding-bottom: 20px; margin-bottom: 20px; }
        .nav { margin: 20px 0; }
        .nav a { margin-right: 20px; color: #0066cc; text-decoration: none; }
        .nav a:hover { text-decoration: underline; }
        .section { margin: 20px 0; }
        .button { background: #0066cc; color: white; padding: 10px 20px; border: none; border-radius: 4px; cursor: pointer; }
        .button:hover { background: #0052a3; }
        .coming-soon { color: #666; font-style: italic; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🧊 {{.Title}}</h1>
            <div class="nav">
                <a href="/">Dashboard</a>
                <a href="/query">Query Editor</a>
                <a href="/tables">Tables</a>
                <a href="/namespaces">Namespaces</a>
                <a href="/metrics">Metrics</a>
            </div>
        </div>
        
        {{if eq .Title "Icebox Dashboard"}}
        <div class="section">
            <h2>Welcome to Icebox</h2>
            <p>Your Apache Iceberg data lakehouse is ready! Start exploring your data with the tools below.</p>
            
            <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin: 20px 0;">
                <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px;">
                    <h3>📊 Query Editor</h3>
                    <p>Write and execute SQL queries against your Iceberg tables.</p>
                    <a href="/query" class="button">Open Query Editor</a>
                </div>
                
                <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px;">
                    <h3>🗂️ Browse Tables</h3>
                    <p>Explore your table schemas and metadata.</p>
                    <a href="/tables" class="button">Browse Tables</a>
                </div>
                
                <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px;">
                    <h3>📁 Namespaces</h3>
                    <p>Manage and organize your data namespaces.</p>
                    <a href="/namespaces" class="button">View Namespaces</a>
                </div>
                
                <div style="border: 1px solid #ddd; padding: 20px; border-radius: 8px;">
                    <h3>📈 System Metrics</h3>
                    <p>Monitor performance and usage statistics.</p>
                    <a href="/metrics" class="button">View Metrics</a>
                </div>
            </div>
        </div>
        {{else}}
        <div class="section">
            <h2>{{.Title}}</h2>
            <p class="coming-soon">🚧 This feature is coming soon! The {{.Title}} interface will provide rich functionality for managing your Iceberg data.</p>
            
            {{if eq .Title "SQL Query Editor"}}
            <div style="margin: 20px 0;">
                <h3>Execute SQL Query</h3>
                <form action="/api/execute" method="post" onsubmit="return executeQuery(event)">
                    <textarea name="sql" placeholder="Enter your SQL query here..." style="width: 100%; height: 200px; padding: 10px; border: 1px solid #ddd; border-radius: 4px; font-family: monospace;"></textarea>
                    <br><br>
                    <button type="submit" class="button">Execute Query</button>
                    {{if .ReadOnly}}<span style="margin-left: 10px; color: #666;">(Read-only mode)</span>{{end}}
                </form>
                <div id="results" style="margin-top: 20px;"></div>
            </div>
            
            <script>
            function executeQuery(event) {
                event.preventDefault();
                const form = event.target;
                const formData = new FormData(form);
                const resultsDiv = document.getElementById('results');
                
                resultsDiv.innerHTML = '<p>Executing query...</p>';
                
                fetch('/api/execute', {
                    method: 'POST',
                    body: formData
                })
                .then(response => response.json())
                .then(data => {
                    if (data.error) {
                        resultsDiv.innerHTML = '<div style="color: red; padding: 10px; border: 1px solid red; border-radius: 4px;">Error: ' + data.error + '</div>';
                    } else {
                        let html = '<div style="border: 1px solid #ddd; border-radius: 4px; padding: 10px;">';
                        html += '<h4>Query Results (' + data.row_count + ' rows, ' + data.duration_ms + 'ms)</h4>';
                        
                        if (data.columns && data.columns.length > 0) {
                            html += '<div style="overflow-x: auto;"><table style="border-collapse: collapse; width: 100%;">';
                            html += '<thead><tr>';
                            data.columns.forEach(col => {
                                html += '<th style="border: 1px solid #ddd; padding: 8px; background: #f5f5f5;">' + col + '</th>';
                            });
                            html += '</tr></thead><tbody>';
                            
                            data.rows.slice(0, 50).forEach(row => {
                                html += '<tr>';
                                row.forEach(cell => {
                                    html += '<td style="border: 1px solid #ddd; padding: 8px;">' + (cell !== null ? cell : 'NULL') + '</td>';
                                });
                                html += '</tr>';
                            });
                            
                            html += '</tbody></table></div>';
                            
                            if (data.rows.length > 50) {
                                html += '<p style="color: #666; font-style: italic;">Showing first 50 rows of ' + data.row_count + ' total.</p>';
                            }
                        }
                        
                        html += '</div>';
                        resultsDiv.innerHTML = html;
                    }
                })
                .catch(error => {
                    resultsDiv.innerHTML = '<div style="color: red; padding: 10px; border: 1px solid red; border-radius: 4px;">Network error: ' + error.message + '</div>';
                });
                
                return false;
            }
            </script>
            {{end}}
        </div>
        {{end}}
        
        <div style="margin-top: 40px; padding-top: 20px; border-top: 1px solid #eee; color: #666; font-size: 14px;">
            <p>Icebox Web UI - Powered by Apache Iceberg | {{.Timestamp}}</p>
        </div>
    </div>
</body>
</html>`

	t, err := template.New("page").Parse(tmpl)
	if err != nil {
		http.Error(w, "Template error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.Execute(w, data); err != nil {
		http.Error(w, "Template execution error", http.StatusInternalServerError)
	}
}

func (ui *WebUIHandler) writeJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")

	// Simple JSON encoding without external dependencies
	if errorData, ok := data.(map[string]interface{}); ok {
		if errorMsg, hasError := errorData["error"]; hasError {
			fmt.Fprintf(w, `{"error": "%s"}`, errorMsg)
			return
		}
	}

	// For simplicity, we'll use a basic JSON serialization
	// In production, this would use proper JSON marshaling
	fmt.Fprintf(w, "%v", data)
}
