package config

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/gear6io/ranger/pkg/errors"
	"github.com/rs/zerolog"
)

// LogManager handles log file rotation and management
type LogManager struct {
	config     *LogConfig
	currentLog *os.File
}

// NewLogManager creates a new log manager
func NewLogManager(cfg *LogConfig) *LogManager {
	return &LogManager{
		config: cfg,
	}
}

// CleanupLogFile clears the log file before starting logging
func CleanupLogFile(filePath string) error {
	if filePath == "" {
		return nil // No file path specified, nothing to clean
	}

	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		return nil // File doesn't exist, nothing to clean
	}

	// Create log directory if it doesn't exist
	logDir := filepath.Dir(filePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return errors.New(ErrLogDirectoryCreationFailed, "failed to create log directory", err)
	}

	// Truncate the file to clear its contents
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		return errors.New(ErrLogFileOpenFailed, "failed to open log file for cleanup", err)
	}
	defer file.Close()

	// The file is now empty
	return nil
}

// GetWriter returns a writer that handles log rotation
func (lm *LogManager) GetWriter() (io.Writer, error) {
	if lm.config.FilePath == "" {
		return nil, errors.New(ErrLogFilePathRequired, "no log file path specified", nil)
	}

	// Create log directory if it doesn't exist
	logDir := filepath.Dir(lm.config.FilePath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, errors.New(ErrLogDirectoryCreationFailed, "failed to create log directory", err)
	}

	// Check if we need to rotate the log file
	if err := lm.checkRotation(); err != nil {
		return nil, errors.New(ErrLogRotationCheckFailed, "failed to check log rotation", err)
	}

	// Open or create the log file
	file, err := os.OpenFile(lm.config.FilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.New(ErrLogFileOpenFailed, "failed to open log file", err)
	}

	lm.currentLog = file
	return file, nil
}

// checkRotation checks if the log file needs to be rotated
func (lm *LogManager) checkRotation() error {
	if lm.config.MaxSize <= 0 {
		return nil // No rotation configured
	}

	// Check if file exists and get its size
	info, err := os.Stat(lm.config.FilePath)
	if os.IsNotExist(err) {
		return nil // File doesn't exist, no rotation needed
	}
	if err != nil {
		return errors.New(ErrLogFileStatFailed, "failed to stat log file", err)
	}

	// Check if file size exceeds max size (convert MB to bytes)
	maxSizeBytes := int64(lm.config.MaxSize) * 1024 * 1024
	if info.Size() < maxSizeBytes {
		return nil // File size is within limits
	}

	// Perform rotation
	return lm.rotateLog()
}

// rotateLog rotates the current log file
func (lm *LogManager) rotateLog() error {
	// Close current log file if open
	if lm.currentLog != nil {
		lm.currentLog.Close()
		lm.currentLog = nil
	}

	// Create backup filename with timestamp
	timestamp := time.Now().Format("2006-01-02-15-04-05")
	backupPath := fmt.Sprintf("%s.%s", lm.config.FilePath, timestamp)

	// Rename current log file to backup
	if err := os.Rename(lm.config.FilePath, backupPath); err != nil {
		return errors.New(ErrLogRotationFailed, "failed to rotate log file", err)
	}

	// Clean up old backup files
	if err := lm.cleanupOldBackups(); err != nil {
		// Log error but don't fail rotation
		fmt.Printf("Warning: failed to cleanup old backups: %v\n", err)
	}

	return nil
}

// cleanupOldBackups removes old backup files based on configuration
func (lm *LogManager) cleanupOldBackups() error {
	if lm.config.MaxBackups <= 0 && lm.config.MaxAge <= 0 {
		return nil // No cleanup configured
	}

	logDir := filepath.Dir(lm.config.FilePath)
	logBase := filepath.Base(lm.config.FilePath)

	// Read directory to find backup files
	entries, err := os.ReadDir(logDir)
	if err != nil {
		return errors.New(ErrLogBackupReadFailed, "failed to read log directory", err)
	}

	var backups []backupInfo
	cutoffTime := time.Now().AddDate(0, 0, -lm.config.MaxAge)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		if !isBackupFile(name, logBase) {
			continue
		}

		info, err := entry.Info()
		if err != nil {
			continue
		}

		backups = append(backups, backupInfo{
			path:    filepath.Join(logDir, name),
			modTime: info.ModTime(),
		})
	}

	// Sort by modification time (oldest first)
	for i := 0; i < len(backups)-1; i++ {
		for j := i + 1; j < len(backups); j++ {
			if backups[i].modTime.After(backups[j].modTime) {
				backups[i], backups[j] = backups[j], backups[i]
			}
		}
	}

	// Remove old backups based on count
	if lm.config.MaxBackups > 0 && len(backups) > lm.config.MaxBackups {
		toRemove := len(backups) - lm.config.MaxBackups
		for i := 0; i < toRemove; i++ {
			if err := os.Remove(backups[i].path); err != nil {
				return errors.New(ErrLogBackupRemoveFailed, "failed to remove old backup", err).AddContext("backup_path", backups[i].path)
			}
		}
	}

	// Remove old backups based on age
	if lm.config.MaxAge > 0 {
		for _, backup := range backups {
			if backup.modTime.Before(cutoffTime) {
				if err := os.Remove(backup.path); err != nil {
					return errors.New(ErrLogBackupRemoveFailed, "failed to remove old backup", err).AddContext("backup_path", backup.path)
				}
			}
		}
	}

	return nil
}

// Close closes the log manager and any open files
func (lm *LogManager) Close() error {
	if lm.currentLog != nil {
		return lm.currentLog.Close()
	}
	return nil
}

// backupInfo holds information about a backup file
type backupInfo struct {
	path    string
	modTime time.Time
}

// isBackupFile checks if a filename is a backup of the main log file
func isBackupFile(name, baseName string) bool {
	return len(name) > len(baseName) && name[:len(baseName)] == baseName && name[len(baseName)] == '.'
}

// SetupLogger creates a configured zerolog logger based on the configuration
func SetupLogger(cfg *Config) (zerolog.Logger, error) {
	// Configure zerolog
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Set log level
	level, err := zerolog.ParseLevel(cfg.Log.Level)
	if err != nil {
		level = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(level)

	// Create writers
	var writers []io.Writer

	// Console writer
	if cfg.Log.Console {
		consoleWriter := zerolog.ConsoleWriter{
			Out:        os.Stdout,
			TimeFormat: time.RFC3339,
		}
		writers = append(writers, consoleWriter)
	}

	// File writer with rotation
	if cfg.Log.FilePath != "" {
		// Clean up the log file before starting logging if enabled
		if cfg.Log.Cleanup {
			if err := CleanupLogFile(cfg.Log.FilePath); err != nil {
				return zerolog.Logger{}, errors.New(ErrLogCleanupFailed, "failed to cleanup log file", err)
			}
		}

		logManager := NewLogManager(&cfg.Log)
		fileWriter, err := logManager.GetWriter()
		if err != nil {
			return zerolog.Logger{}, errors.New(ErrLogFileWriterSetupFailed, "failed to setup file writer", err)
		}

		// Add file writer directly (zerolog will handle JSON formatting)
		writers = append(writers, fileWriter)
	}

	// Create multi-writer
	var multiWriter io.Writer
	if len(writers) == 1 {
		multiWriter = writers[0]
	} else {
		multiWriter = zerolog.MultiLevelWriter(writers...)
	}

	// Create logger
	logger := zerolog.New(multiWriter).With().
		Timestamp().
		Str("component", "ranger-server").
		Logger()

	return logger, nil
}
