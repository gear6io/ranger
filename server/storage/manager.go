package storage

import (
	"context"
	"fmt"
	"io"

	"github.com/TFMV/icebox/server/storage/local"
	"github.com/TFMV/icebox/server/storage/memory"
	"github.com/TFMV/icebox/server/storage/minio"
	"github.com/rs/zerolog"
)

// Manager represents a data storage manager for icebox
type Manager struct {
	config *Config
	logger zerolog.Logger
	fs     FileSystem
}

// Config holds data storage configuration
type Config struct {
	Type string `yaml:"type"` // filesystem, s3, memory
	Path string `yaml:"path"` // for filesystem storage
}

// FileSystem interface for data storage operations
type FileSystem interface {
	Open(path string) (io.ReadCloser, error)
	Create(path string) (io.WriteCloser, error)
	Remove(path string) error
	Exists(path string) (bool, error)
}

// NewManager creates a new data storage manager
func NewManager(cfg *Config, logger zerolog.Logger) (*Manager, error) {
	var fs FileSystem
	var err error

	switch cfg.Type {
	case "filesystem":
		fs, err = local.NewLocalFileSystem(cfg.Path)
	case "memory":
		fs, err = memory.NewFileSystemAdapter()
	case "s3":
		fs, err = minio.NewS3FileSystem(cfg)
	default:
		return nil, fmt.Errorf("unsupported storage type: %s", cfg.Type)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to create filesystem: %w", err)
	}

	return &Manager{
		config: cfg,
		logger: logger,
		fs:     fs,
	}, nil
}

// Initialize initializes the data storage
func (m *Manager) Initialize(ctx context.Context) error {
	m.logger.Info().Str("type", m.config.Type).Msg("Initializing data storage")
	// TODO: Implement actual storage initialization
	return nil
}

// Close closes the data storage
func (m *Manager) Close() error {
	m.logger.Info().Msg("Closing data storage")
	// TODO: Implement actual storage cleanup
	return nil
}

// GetStatus returns data storage status
func (m *Manager) GetStatus() map[string]interface{} {
	return map[string]interface{}{
		"type": m.config.Type,
		"path": m.config.Path,
	}
}

// GetFileSystem returns the underlying filesystem
func (m *Manager) GetFileSystem() FileSystem {
	return m.fs
}
