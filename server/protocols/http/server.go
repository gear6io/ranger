package http

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gear6io/ranger/server/config"
	"github.com/gear6io/ranger/server/query"
	"github.com/rs/zerolog"
)

// Server represents the HTTP protocol server
type Server struct {
	queryEngine *query.Engine
	logger      zerolog.Logger
	server      *http.Server
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewServer creates a new HTTP server instance
func NewServer(queryEngine *query.Engine, logger zerolog.Logger) (*Server, error) {
	ctx, cancel := context.WithCancel(context.Background())

	return &Server{
		queryEngine: queryEngine,
		logger:      logger.With().Str("component", "http-server").Logger(),
		ctx:         ctx,
		cancel:      cancel,
	}, nil
}

// Start starts the HTTP server
func (s *Server) Start(ctx context.Context) error {
	if !config.HTTP_SERVER_ENABLED {
		s.logger.Info().Msg("HTTP server is disabled")
		return nil
	}

	// Use fixed port and address from config constants
	port := config.HTTP_SERVER_PORT
	addr := fmt.Sprintf("%s:%d", config.DEFAULT_SERVER_ADDRESS, port)
	s.logger.Info().Str("address", addr).Msg("Starting HTTP server")

	// Create HTTP server with query handling
	mux := http.NewServeMux()

	// Add query endpoint
	mux.HandleFunc("/query", s.handleQuery)

	// Add status endpoint
	mux.HandleFunc("/status", s.handleStatus)

	// Add server info endpoint
	mux.HandleFunc("/info", s.handleInfo)

	// Add health check endpoint
	mux.HandleFunc("/health", s.handleHealth)

	s.server = &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	// Start server in goroutine
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.logger.Error().Err(err).Msg("HTTP server error")
		}
	}()

	s.logger.Info().Msg("HTTP server started successfully")
	return nil
}

// handleQuery handles SQL query requests
func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query from request body
	queryStr := r.URL.Query().Get("q")
	if queryStr == "" {
		http.Error(w, "Missing query parameter 'q'", http.StatusBadRequest)
		return
	}

	s.logger.Info().Str("query", queryStr).Msg("Executing query via HTTP")

	// Execute query using QueryEngine
	result, err := s.queryEngine.ExecuteQuery(r.Context(), queryStr)
	if err != nil {
		s.logger.Error().Err(err).Str("query", queryStr).Msg("Query execution failed")
		http.Error(w, fmt.Sprintf("Query execution failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Return comprehensive result as JSON
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Create a proper JSON response structure
	response := map[string]interface{}{
		"status":    "success",
		"query":     queryStr,
		"rowCount":  result.RowCount,
		"columns":   result.Columns,
		"message":   result.Message,
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	}

	// Add data if available
	if result.Data != nil {
		if rows, ok := result.Data.([][]interface{}); ok {
			response["data"] = rows
		} else {
			response["data"] = result.Data
		}
	}

	// Convert to JSON
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to marshal JSON response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Write(jsonResponse)
}

// handleStatus handles status requests
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	// Simple status response
	response := fmt.Sprintf(`{"status":"running","server":"http"}`)
	w.Write([]byte(response))
}

// handleInfo handles server information requests
func (s *Server) handleInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	info := map[string]interface{}{
		"server":      "ranger-http",
		"version":     "0.1.0",
		"protocol":    "HTTP/1.1",
		"queryEngine": "enabled",
		"endpoints": []string{
			"POST /query - Execute SQL queries",
			"GET /status - Server status",
			"GET /info - Server information",
			"GET /health - Health check",
		},
	}

	jsonResponse, err := json.Marshal(info)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to marshal info response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Write(jsonResponse)
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	health := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
		"server":    "ranger-http",
	}

	jsonResponse, err := json.Marshal(health)
	if err != nil {
		s.logger.Error().Err(err).Msg("Failed to marshal health response")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	w.Write(jsonResponse)
}

// Stop stops the HTTP server
func (s *Server) Stop() error {
	s.logger.Info().Msg("Stopping HTTP server")

	s.cancel()

	if s.server != nil {
		// Create a context with timeout for graceful shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		if err := s.server.Shutdown(shutdownCtx); err != nil {
			s.logger.Error().Err(err).Msg("Error during HTTP server shutdown")
		}
	}

	// Wait for all goroutines to finish
	s.wg.Wait()

	s.logger.Info().Msg("HTTP server stopped")
	return nil
}

// GetStatus returns server status
func (s *Server) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"enabled": config.HTTP_SERVER_ENABLED,
		"address": config.DEFAULT_SERVER_ADDRESS,
		"port":    config.HTTP_SERVER_PORT,
	}
}
