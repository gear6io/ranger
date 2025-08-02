//go:build deprecated
// +build deprecated

// DEPRECATED: This file has been migrated to server/protocols/jdbc/
// Use icebox-server for JDBC functionality
// This file will be removed in a future release
//
// ⚠️  WARNING: This CLI command is deprecated
// 🚀 NEW: Use ./icebox-server for JDBC server functionality
// 📖 See MIGRATION.md for migration instructions

package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TFMV/icebox/deprecated/catalog"
	"github.com/TFMV/icebox/deprecated/catalog/sqlite"
	"github.com/TFMV/icebox/deprecated/config"
	"github.com/TFMV/icebox/deprecated/engine/duckdb"
	"github.com/TFMV/icebox/deprecated/engine/jdbc"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

// DEPRECATED: This command has been moved to icebox-server
// Use 'icebox-server' instead for JDBC functionality
var jdbcCmd = &cobra.Command{
	Use:   "jdbc",
	Short: "Start JDBC server (DEPRECATED - use icebox-server)",
	Long: `Start JDBC server for PostgreSQL wire protocol connections.
	
DEPRECATED: This command has been moved to icebox-server.
Use 'icebox-server' instead for JDBC functionality.

This will be removed in a future release.`,
	RunE: runJDBC,
}

type jdbcOptions struct {
	port              int
	host              string
	maxConnections    int
	connectionTimeout time.Duration
	queryTimeout      time.Duration
	verbose           bool
	logLevel          string
	certFile          string
	keyFile           string
	enableSSL         bool
}

var jdbcOpts = &jdbcOptions{}

// JDBCServer represents the JDBC server instance
type JDBCServer struct {
	config          *config.Config
	catalog         catalog.CatalogInterface
	engine          *duckdb.Engine
	handler         *jdbc.JDBCHandler
	listener        net.Listener
	connections     map[string]*JDBCConnection
	maxConnections  int
	connectionCount int
	logger          zerolog.Logger
	ctx             context.Context
	cancel          context.CancelFunc
}

// JDBCConnection represents a client connection
type JDBCConnection struct {
	ID           string
	ConnectedAt  time.Time
	LastActivity time.Time
	Database     string
	Username     string
	IsActive     bool
}

// DEPRECATED: This function has been removed as this file is deprecated
// Use icebox-server instead for JDBC functionality

func runJDBC(cmd *cobra.Command, args []string) error {
	// DEPRECATED: This function is deprecated, use icebox-server instead
	fmt.Fprintf(os.Stderr, "⚠️  DEPRECATED: Use 'icebox-server' instead of 'icebox jdbc'\n")

	// Create logger directly since this is deprecated
	log := zerolog.New(os.Stdout).With().Timestamp().Logger()
	logger := &log
	ctx := cmd.Context()

	// Load configuration
	_, cfg, err := config.FindConfig()
	if err != nil {
		return fmt.Errorf("❌ Failed to load configuration: %w", err)
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
		return fmt.Errorf("❌ JDBC server currently only supports SQLite catalogs")
	}
	defer engine.Close()

	// Create JDBC handler
	handler := jdbc.NewJDBCHandler(engine, *logger, ctx)

	// Create JDBC server
	server := &JDBCServer{
		config:         cfg,
		catalog:        cat,
		engine:         engine,
		handler:        handler,
		connections:    make(map[string]*JDBCConnection),
		maxConnections: jdbcOpts.maxConnections,
		logger:         *logger,
	}
	server.ctx, server.cancel = context.WithCancel(ctx)

	// Start server
	return server.Start()
}

// Start starts the JDBC server
func (s *JDBCServer) Start() error {
	addr := fmt.Sprintf("%s:%d", jdbcOpts.host, jdbcOpts.port)

	// Create listener
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("❌ Failed to listen on %s: %w", addr, err)
	}
	s.listener = listener

	s.logger.Info().
		Str("address", addr).
		Int("max_connections", s.maxConnections).
		Msg("Starting JDBC server")

	// Print connection information
	fmt.Printf("🚀 Icebox JDBC Server starting...\n")
	fmt.Printf("📍 Listening on: %s\n", addr)
	fmt.Printf("🔗 Max connections: %d\n", s.maxConnections)
	fmt.Printf("⏱️  Connection timeout: %v\n", jdbcOpts.connectionTimeout)
	fmt.Printf("⏱️  Query timeout: %v\n", jdbcOpts.queryTimeout)

	if jdbcOpts.enableSSL {
		fmt.Printf("🔒 SSL/TLS: Enabled\n")
	}

	fmt.Printf("\n📋 Connection Examples:\n")
	fmt.Printf("   JDBC URL: jdbc:postgresql://localhost:%d/icebox\n", jdbcOpts.port)
	fmt.Printf("   Username: (any)\n")
	fmt.Printf("   Password: (any)\n")
	fmt.Printf("\n✅ JDBC server ready for connections\n")

	// Start connection handler
	go s.handleConnections()

	// Graceful shutdown
	go func() {
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
		<-quit

		s.logger.Info().Msg("Shutting down JDBC server...")
		fmt.Printf("\n🛑 Shutting down JDBC server...\n")

		s.cancel()
		s.listener.Close()

		// Close all active connections
		for _, conn := range s.connections {
			if conn.IsActive {
				s.logger.Debug().Str("connection_id", conn.ID).Msg("Closing connection")
			}
		}

		fmt.Printf("✅ JDBC server stopped\n")
	}()

	// Wait for context cancellation
	<-s.ctx.Done()
	return nil
}

// handleConnections accepts and handles incoming connections
func (s *JDBCServer) handleConnections() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if s.ctx.Err() != nil {
				// Server is shutting down
				return
			}
			s.logger.Error().Err(err).Msg("Failed to accept connection")
			continue
		}

		// Check connection limit
		if s.connectionCount >= s.maxConnections {
			s.logger.Warn().Msg("Connection limit reached, rejecting connection")
			conn.Close()
			continue
		}

		// Handle connection in goroutine
		go s.handleConnection(conn)
	}
}

// handleConnection handles a single client connection
func (s *JDBCServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	connectionID := fmt.Sprintf("%s-%d", conn.RemoteAddr().String(), time.Now().UnixNano())

	connection := &JDBCConnection{
		ID:           connectionID,
		ConnectedAt:  time.Now(),
		LastActivity: time.Now(),
		IsActive:     true,
	}

	s.connections[connectionID] = connection
	s.connectionCount++

	s.logger.Info().
		Str("connection_id", connectionID).
		Str("remote_addr", conn.RemoteAddr().String()).
		Msg("New connection established")

	defer func() {
		connection.IsActive = false
		delete(s.connections, connectionID)
		s.connectionCount--

		s.logger.Info().
			Str("connection_id", connectionID).
			Msg("Connection closed")
	}()

	// Set connection timeout
	conn.SetDeadline(time.Now().Add(jdbcOpts.connectionTimeout))

	// Use the JDBC handler to handle the connection
	if err := s.handler.HandleConnection(conn); err != nil {
		s.logger.Error().
			Str("connection_id", connectionID).
			Err(err).
			Msg("Connection handler error")
	}
}

// GetConnectionStats returns current connection statistics
func (s *JDBCServer) GetConnectionStats() map[string]interface{} {
	activeConnections := 0
	for _, conn := range s.connections {
		if conn.IsActive {
			activeConnections++
		}
	}

	return map[string]interface{}{
		"total_connections":     s.connectionCount,
		"active_connections":    activeConnections,
		"max_connections":       s.maxConnections,
		"available_connections": s.maxConnections - s.connectionCount,
	}
}
