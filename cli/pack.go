package cli

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/TFMV/icebox/config"
	"github.com/TFMV/icebox/display"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var packCmd = &cobra.Command{
	Use:   "pack [directory]",
	Short: "Create a shareable archive of an Icebox project",
	Long: `Create a compressed archive (.tar.gz) of an Icebox project for sharing or backup.

The pack command creates a self-contained archive that includes:
- Icebox configuration (.icebox.yml)
- Catalog metadata (SQLite database or REST configuration)
- Table data files (optional, use --include-data)
- Manifest with checksums for integrity verification

This enables:
- Sharing complete lakehouse examples
- Creating reproducible demo environments
- Backing up Icebox projects
- Distributing sample datasets

Examples:
  icebox pack                              # Pack current project
  icebox pack my-project                   # Pack specific project directory
  icebox pack --include-data               # Include all table data files
  icebox pack --output demo.tar.gz         # Custom output filename
  icebox pack --checksum --compress       # Full integrity checking`,
	Args: cobra.MaximumNArgs(1),
	RunE: runPack,
}

var unpackCmd = &cobra.Command{
	Use:   "unpack <archive>",
	Short: "Extract an Icebox project archive",
	Long: `Extract a packed Icebox project archive and verify its integrity.

The unpack command:
- Extracts the archive to a target directory
- Verifies file checksums if manifest is present
- Restores the complete Icebox project structure
- Validates configuration files

Examples:
  icebox unpack demo.tar.gz                # Extract to current directory
  icebox unpack demo.tar.gz --dir myproject # Extract to specific directory
  icebox unpack demo.tar.gz --verify       # Verify checksums during extraction`,
	Args: cobra.ExactArgs(1),
	RunE: runUnpack,
}

type packOptions struct {
	includeData bool
	output      string
	checksum    bool
	compress    bool
	exclude     []string
	maxSize     int64
}

type unpackOptions struct {
	targetDir string
	verify    bool
	overwrite bool
	skipData  bool
}

var (
	packOpts   = &packOptions{}
	unpackOpts = &unpackOptions{}
)

// PackageManifest represents the contents and metadata of a packed project
type PackageManifest struct {
	PackageInfo `json:"package_info"`
	Files       map[string]FileInfo `json:"files"`
	Config      interface{}         `json:"config,omitempty"`
}

type PackageInfo struct {
	Name        string    `json:"name"`
	Version     string    `json:"version"`
	CreatedAt   time.Time `json:"created_at"`
	CreatedBy   string    `json:"created_by"`
	TotalSize   int64     `json:"total_size"`
	FileCount   int       `json:"file_count"`
	IncludeData bool      `json:"include_data"`
	Description string    `json:"description,omitempty"`
}

type FileInfo struct {
	Path     string `json:"path"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"`
	Mode     uint32 `json:"mode"`
}

func init() {
	rootCmd.AddCommand(packCmd)
	rootCmd.AddCommand(unpackCmd)

	// Pack command flags
	packCmd.Flags().BoolVar(&packOpts.includeData, "include-data", false, "include table data files in archive")
	packCmd.Flags().StringVar(&packOpts.output, "output", "", "output archive filename (default: <project-name>.tar.gz)")
	packCmd.Flags().BoolVar(&packOpts.checksum, "checksum", true, "generate checksums for integrity verification")
	packCmd.Flags().BoolVar(&packOpts.compress, "compress", true, "compress the archive with gzip")
	packCmd.Flags().StringSliceVar(&packOpts.exclude, "exclude", nil, "exclude patterns (glob)")
	packCmd.Flags().Int64Var(&packOpts.maxSize, "max-size", 1024*1024*1024, "maximum archive size in bytes (1GB default)")

	// Unpack command flags
	unpackCmd.Flags().StringVar(&unpackOpts.targetDir, "dir", "", "target directory for extraction (default: current directory)")
	unpackCmd.Flags().BoolVar(&unpackOpts.verify, "verify", true, "verify file checksums during extraction")
	unpackCmd.Flags().BoolVar(&unpackOpts.overwrite, "overwrite", false, "overwrite existing files")
	unpackCmd.Flags().BoolVar(&unpackOpts.skipData, "skip-data", false, "skip data files during extraction")
}

func runPack(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "pack").Msg("Starting pack operation")
	}

	// Determine project directory
	projectDir := "."
	if len(args) > 0 {
		projectDir = args[0]
	}

	if logger != nil {
		logger.Debug().Str("project_dir", projectDir).Msg("Using project directory")
	}

	// Make path absolute
	absProjectDir, err := filepath.Abs(projectDir)
	if err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("project_dir", projectDir).Msg("Failed to resolve project directory")
		}
		d.Error("Failed to resolve project directory: %v", err)
		return fmt.Errorf("❌ Failed to resolve project directory: %w", err)
	}

	// Find and load configuration
	// Change to project directory temporarily to find config
	originalDir, err := os.Getwd()
	if err != nil {
		if logger != nil {
			logger.Error().Err(err).Msg("Failed to get current directory")
		}
		d.Error("Failed to get current directory: %v", err)
		return fmt.Errorf("❌ Failed to get current directory: %w", err)
	}

	if err := os.Chdir(absProjectDir); err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("project_dir", absProjectDir).Msg("Failed to change to project directory")
		}
		d.Error("Failed to change to project directory: %v", err)
		return fmt.Errorf("❌ Failed to change to project directory: %w", err)
	}

	configPath, cfg, err := config.FindConfig()

	// Change back to original directory
	if chErr := os.Chdir(originalDir); chErr != nil {
		// Log the error but don't fail the operation
		if logger != nil {
			logger.Warn().Err(chErr).Msg("Failed to change back to original directory")
		}
		d.Warning("Failed to change back to original directory: %v", chErr)
	}

	if err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("project_dir", absProjectDir).Msg("Failed to find Icebox configuration")
		}
		d.Error("Failed to find Icebox configuration in %s", absProjectDir)
		d.Info("Try running 'icebox init' first to create a new project")
		return fmt.Errorf("❌ Failed to find Icebox configuration in %s\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", absProjectDir, err)
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		d.Info("Packing project: %s", absProjectDir)
		d.Info("Using configuration: %s", configPath)
		if logger != nil {
			logger.Info().Str("project_dir", absProjectDir).Str("config_path", configPath).Msg("Pack configuration details")
		}
	}

	// Determine output filename
	outputPath := packOpts.output
	if outputPath == "" {
		projectName := cfg.Name
		if projectName == "" {
			projectName = filepath.Base(absProjectDir)
		}
		outputPath = fmt.Sprintf("%s.tar.gz", projectName)
	}

	if logger != nil {
		logger.Info().Str("output_path", outputPath).Bool("include_data", packOpts.includeData).Msg("Creating archive")
	}

	// Create the archive
	if err := createArchive(ctx, absProjectDir, outputPath, cfg, d, logger); err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("output_path", outputPath).Msg("Failed to create archive")
		}
		d.Error("Failed to create archive: %v", err)
		return fmt.Errorf("❌ Failed to create archive: %w", err)
	}

	// Display success message
	fileInfo, _ := os.Stat(outputPath)
	d.Success("Successfully created archive!")

	// Create a table to display archive details
	tableData := display.TableData{
		Headers: []string{"Property", "Value"},
		Rows: [][]interface{}{
			{"File", outputPath},
			{"Includes data", packOpts.includeData},
			{"Checksums", packOpts.checksum},
		},
	}

	if fileInfo != nil {
		tableData.Rows = append(tableData.Rows, []interface{}{"Size", display.FormatBytes(fileInfo.Size())})
	}

	table := d.Table(tableData).
		WithTitle("Archive Details").
		WithCompactMode()

	if err := table.Render(); err != nil {
		if logger != nil {
			logger.Warn().Err(err).Msg("Failed to render archive details table")
		}
		// Fallback to simple output
		d.Info("Archive Details:")
		d.Info("   File: %s", outputPath)
		if fileInfo != nil {
			d.Info("   Size: %s", display.FormatBytes(fileInfo.Size()))
		}
		d.Info("   Includes data: %v", packOpts.includeData)
		d.Info("   Checksums: %v", packOpts.checksum)
	}

	if logger != nil {
		logger.Info().Str("cmd", "pack").Str("output_path", outputPath).Msg("Pack operation completed successfully")
	}

	return nil
}

func runUnpack(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()
	d := getDisplayFromContext(ctx)
	logger := getLoggerFromContext(ctx)

	if logger != nil {
		logger.Info().Str("cmd", "unpack").Msg("Starting unpack operation")
	}

	archivePath := args[0]

	if logger != nil {
		logger.Debug().Str("archive_path", archivePath).Msg("Using archive path")
	}

	// Determine target directory
	targetDir := unpackOpts.targetDir
	if targetDir == "" {
		targetDir = "."
	}

	// Make path absolute
	absTargetDir, err := filepath.Abs(targetDir)
	if err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("target_dir", targetDir).Msg("Failed to resolve target directory")
		}
		d.Error("Failed to resolve target directory: %v", err)
		return fmt.Errorf("❌ Failed to resolve target directory: %w", err)
	}

	if logger != nil {
		logger.Info().Str("archive_path", archivePath).Str("target_dir", absTargetDir).Bool("verify", unpackOpts.verify).Msg("Extracting archive")
	}

	// Extract the archive
	if err := extractArchive(ctx, archivePath, absTargetDir, d, logger); err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("archive_path", archivePath).Msg("Failed to extract archive")
		}
		d.Error("Failed to extract archive: %v", err)
		return fmt.Errorf("❌ Failed to extract archive: %w", err)
	}

	// Display success message
	d.Success("Successfully extracted archive!")

	// Create a table to display extraction details
	tableData := display.TableData{
		Headers: []string{"Property", "Value"},
		Rows: [][]interface{}{
			{"Archive", archivePath},
			{"Target", absTargetDir},
			{"Verified", unpackOpts.verify},
		},
	}

	table := d.Table(tableData).
		WithTitle("Extraction Details").
		WithCompactMode()

	if err := table.Render(); err != nil {
		if logger != nil {
			logger.Warn().Err(err).Msg("Failed to render extraction details table")
		}
		// Fallback to simple output
		d.Info("Extraction Details:")
		d.Info("   Archive: %s", archivePath)
		d.Info("   Target: %s", absTargetDir)
		d.Info("   Verified: %v", unpackOpts.verify)
	}

	if logger != nil {
		logger.Info().Str("cmd", "unpack").Str("archive_path", archivePath).Msg("Unpack operation completed successfully")
	}

	return nil
}

func createArchive(ctx context.Context, projectDir, outputPath string, cfg *config.Config, d display.Display, logger *zerolog.Logger) error {
	if logger != nil {
		logger.Info().Str("project_dir", projectDir).Str("output_path", outputPath).Msg("Starting archive creation")
	}

	// Create output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("output_path", outputPath).Msg("Failed to create output file")
		}
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outputFile.Close()

	// Create writer chain
	var writer io.Writer = outputFile
	var gzipWriter *gzip.Writer

	if packOpts.compress {
		gzipWriter = gzip.NewWriter(outputFile)
		writer = gzipWriter
		defer gzipWriter.Close()
		if logger != nil {
			logger.Debug().Msg("Using gzip compression")
		}
	}

	tarWriter := tar.NewWriter(writer)
	defer tarWriter.Close()

	// Collect files to archive
	manifest := &PackageManifest{
		PackageInfo: PackageInfo{
			Name:        cfg.Name,
			Version:     getBuildVersion(),
			CreatedAt:   time.Now(),
			CreatedBy:   "icebox",
			IncludeData: packOpts.includeData,
		},
		Files:  make(map[string]FileInfo),
		Config: cfg,
	}

	var totalSize int64
	fileCount := 0

	if logger != nil {
		logger.Debug().Str("project_dir", projectDir).Msg("Walking project directory")
	}

	// Walk the project directory
	err = filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if logger != nil {
				logger.Warn().Err(err).Str("path", path).Msg("Error walking path")
			}
			return err
		}

		// Skip if it's the output file itself
		if path == outputPath {
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(projectDir, path)
		if err != nil {
			if logger != nil {
				logger.Error().Err(err).Str("path", path).Msg("Failed to get relative path")
			}
			return err
		}

		// Skip certain files/directories
		if shouldSkip(relPath, info) {
			if logger != nil {
				logger.Debug().Str("path", relPath).Msg("Skipping file/directory")
			}
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip data files if not including data
		if !packOpts.includeData && isDataFile(relPath) {
			if logger != nil {
				logger.Debug().Str("path", relPath).Msg("Skipping data file")
			}
			return nil
		}

		// Check size limits
		if totalSize+info.Size() > packOpts.maxSize {
			if logger != nil {
				logger.Error().Int64("current_size", totalSize).Int64("file_size", info.Size()).Int64("max_size", packOpts.maxSize).Msg("Archive would exceed size limit")
			}
			return fmt.Errorf("archive would exceed maximum size limit (%s)", display.FormatBytes(packOpts.maxSize))
		}

		// Add file to archive
		if !info.IsDir() {
			if err := addFileToArchive(tarWriter, path, relPath, info, manifest); err != nil {
				if logger != nil {
					logger.Error().Err(err).Str("path", relPath).Msg("Failed to add file to archive")
				}
				return fmt.Errorf("failed to add file %s: %w", relPath, err)
			}
			totalSize += info.Size()
			fileCount++
			if logger != nil {
				logger.Debug().Str("path", relPath).Int64("size", info.Size()).Msg("Added file to archive")
			}
		} else {
			// Add directory entry
			if err := addDirToArchive(tarWriter, relPath, info); err != nil {
				if logger != nil {
					logger.Error().Err(err).Str("path", relPath).Msg("Failed to add directory to archive")
				}
				return fmt.Errorf("failed to add directory %s: %w", relPath, err)
			}
			if logger != nil {
				logger.Debug().Str("path", relPath).Msg("Added directory to archive")
			}
		}

		return nil
	})

	if err != nil {
		if logger != nil {
			logger.Error().Err(err).Msg("Failed to walk project directory")
		}
		return fmt.Errorf("failed to walk project directory: %w", err)
	}

	// Update manifest totals
	manifest.PackageInfo.TotalSize = totalSize
	manifest.PackageInfo.FileCount = fileCount

	// Add manifest to archive
	if err := addManifestToArchive(tarWriter, manifest); err != nil {
		if logger != nil {
			logger.Error().Err(err).Msg("Failed to add manifest to archive")
		}
		return fmt.Errorf("failed to add manifest: %w", err)
	}

	d.Info("Packed %d files (%s total)", fileCount, display.FormatBytes(totalSize))

	if logger != nil {
		logger.Info().Int("file_count", fileCount).Int64("total_size", totalSize).Msg("Archive creation completed")
	}

	return nil
}

func extractArchive(ctx context.Context, archivePath, targetDir string, d display.Display, logger *zerolog.Logger) error {
	if logger != nil {
		logger.Info().Str("archive_path", archivePath).Str("target_dir", targetDir).Msg("Starting archive extraction")
	}

	// Open archive file
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		if logger != nil {
			logger.Error().Err(err).Str("archive_path", archivePath).Msg("Failed to open archive file")
		}
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer archiveFile.Close()

	// Create reader chain
	var reader io.Reader = archiveFile

	// Check if it's gzipped
	if strings.HasSuffix(archivePath, ".gz") {
		gzipReader, err := gzip.NewReader(archiveFile)
		if err != nil {
			if logger != nil {
				logger.Error().Err(err).Msg("Failed to create gzip reader")
			}
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
		if logger != nil {
			logger.Debug().Msg("Using gzip decompression")
		}
	}

	tarReader := tar.NewReader(reader)

	var manifest *PackageManifest
	extractedFiles := 0

	if logger != nil {
		logger.Debug().Msg("Starting file extraction")
	}

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if logger != nil {
				logger.Error().Err(err).Msg("Failed to read tar header")
			}
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		// Validate and sanitize the path to prevent zip slip attacks
		targetPath, err := sanitizeExtractPath(targetDir, header.Name)
		if err != nil {
			if logger != nil {
				logger.Error().Err(err).Str("entry_name", header.Name).Msg("Invalid archive entry")
			}
			return fmt.Errorf("invalid archive entry '%s': %w", header.Name, err)
		}

		// Handle manifest
		if header.Name == "manifest.json" {
			manifestData, err := io.ReadAll(tarReader)
			if err != nil {
				if logger != nil {
					logger.Error().Err(err).Msg("Failed to read manifest")
				}
				return fmt.Errorf("failed to read manifest: %w", err)
			}
			if err := json.Unmarshal(manifestData, &manifest); err != nil {
				if logger != nil {
					logger.Error().Err(err).Msg("Failed to parse manifest")
				}
				return fmt.Errorf("failed to parse manifest: %w", err)
			}
			if logger != nil {
				logger.Debug().Msg("Loaded manifest from archive")
			}
			continue
		}

		// Skip data files if requested
		if unpackOpts.skipData && isDataFile(header.Name) {
			if logger != nil {
				logger.Debug().Str("file", header.Name).Msg("Skipping data file")
			}
			continue
		}

		// Check if file exists and handle overwrite
		if _, err := os.Stat(targetPath); err == nil && !unpackOpts.overwrite {
			d.Warning("Skipping existing file: %s (use --overwrite to replace)", header.Name)
			if logger != nil {
				logger.Debug().Str("file", header.Name).Msg("Skipping existing file")
			}
			continue
		}

		// Create target directory
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			if logger != nil {
				logger.Error().Err(err).Str("dir", filepath.Dir(targetPath)).Msg("Failed to create directory")
			}
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Extract based on type
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				if logger != nil {
					logger.Error().Err(err).Str("dir", targetPath).Msg("Failed to create directory")
				}
				return fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}
			if logger != nil {
				logger.Debug().Str("dir", header.Name).Msg("Created directory")
			}

		case tar.TypeReg:
			if err := extractFile(tarReader, targetPath, header, manifest); err != nil {
				if logger != nil {
					logger.Error().Err(err).Str("file", header.Name).Msg("Failed to extract file")
				}
				return fmt.Errorf("failed to extract file %s: %w", header.Name, err)
			}
			extractedFiles++
			if logger != nil {
				logger.Debug().Str("file", header.Name).Int64("size", header.Size).Msg("Extracted file")
			}
		}
	}

	d.Info("Extracted %d files", extractedFiles)

	if logger != nil {
		logger.Info().Int("extracted_files", extractedFiles).Msg("Archive extraction completed")
	}

	return nil
}

func addFileToArchive(tarWriter *tar.Writer, filePath, relPath string, info os.FileInfo, manifest *PackageManifest) error {
	// Open file
	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Calculate checksum if requested
	var checksum string
	if packOpts.checksum {
		hasher := sha256.New()
		if _, err := io.Copy(hasher, file); err != nil {
			return fmt.Errorf("failed to calculate checksum: %w", err)
		}
		checksum = fmt.Sprintf("%x", hasher.Sum(nil))
		if _, err := file.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to reset file position: %w", err)
		} // Reset for actual copy
	}

	// Create tar header
	header := &tar.Header{
		Name:    relPath,
		Mode:    int64(info.Mode()),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}

	// Write header
	if err := tarWriter.WriteHeader(header); err != nil {
		return err
	}

	// Copy file content
	if _, err := io.Copy(tarWriter, file); err != nil {
		return err
	}

	// Add to manifest
	manifest.Files[relPath] = FileInfo{
		Path:     relPath,
		Size:     info.Size(),
		Checksum: checksum,
		Mode:     uint32(info.Mode()),
	}

	return nil
}

func addDirToArchive(tarWriter *tar.Writer, relPath string, info os.FileInfo) error {
	header := &tar.Header{
		Name:     relPath + "/",
		Mode:     int64(info.Mode()),
		Typeflag: tar.TypeDir,
		ModTime:  info.ModTime(),
	}

	return tarWriter.WriteHeader(header)
}

func addManifestToArchive(tarWriter *tar.Writer, manifest *PackageManifest) error {
	manifestData, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}

	header := &tar.Header{
		Name: "manifest.json",
		Mode: 0644,
		Size: int64(len(manifestData)),
	}

	if err := tarWriter.WriteHeader(header); err != nil {
		return err
	}

	_, err = tarWriter.Write(manifestData)
	return err
}

func extractFile(tarReader *tar.Reader, targetPath string, header *tar.Header, manifest *PackageManifest) error {
	// Create file
	outFile, err := os.Create(targetPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	// Copy content
	hasher := sha256.New()
	var writer io.Writer = outFile

	if unpackOpts.verify && manifest != nil {
		writer = io.MultiWriter(outFile, hasher)
	}

	if _, err := io.Copy(writer, tarReader); err != nil {
		return err
	}

	// Set file mode
	if err := os.Chmod(targetPath, os.FileMode(header.Mode)); err != nil {
		return err
	}

	// Verify checksum if enabled
	if unpackOpts.verify && manifest != nil {
		if fileInfo, exists := manifest.Files[header.Name]; exists && fileInfo.Checksum != "" {
			actualChecksum := fmt.Sprintf("%x", hasher.Sum(nil))
			if actualChecksum != fileInfo.Checksum {
				return fmt.Errorf("checksum mismatch for %s: expected %s, got %s",
					header.Name, fileInfo.Checksum, actualChecksum)
			}
		}
	}

	return nil
}

func shouldSkip(relPath string, info os.FileInfo) bool {
	// Skip hidden files and directories (except .icebox.yml)
	if strings.HasPrefix(filepath.Base(relPath), ".") && relPath != ".icebox.yml" {
		return true
	}

	// Skip common build/temp directories
	skipDirs := []string{"node_modules", ".git", ".vscode", ".idea", "target", "build"}
	for _, skipDir := range skipDirs {
		if strings.Contains(relPath, skipDir) {
			return true
		}
	}

	// Check exclude patterns
	for _, pattern := range packOpts.exclude {
		if matched, _ := filepath.Match(pattern, relPath); matched {
			return true
		}
	}

	return false
}

func isDataFile(relPath string) bool {
	// Check if the file is in a data directory
	return strings.Contains(relPath, ".icebox/data/") ||
		strings.HasSuffix(relPath, ".parquet") ||
		strings.HasSuffix(relPath, ".avro")
}

// getBuildVersion returns the version from build info or a default version
func getBuildVersion() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		// Try to get version from VCS info first
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && len(setting.Value) >= 7 {
				return "dev-" + setting.Value[:7] // Short commit hash
			}
		}

		// Fall back to module version if available
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			return info.Main.Version
		}
	}

	// Default version if no build info available
	return "0.1.0-dev"
}

// sanitizeExtractPath validates and sanitizes archive entry paths to prevent zip slip attacks
func sanitizeExtractPath(targetDir, entryName string) (string, error) {
	// Clean the entry name to normalize path separators and remove redundant elements
	cleanName := filepath.Clean(entryName)

	// Check for absolute paths (both Unix and Windows style)
	if filepath.IsAbs(cleanName) || isWindowsAbsolutePath(cleanName) {
		return "", fmt.Errorf("absolute paths not allowed in archives")
	}

	// Check for path traversal attempts
	if strings.Contains(cleanName, "..") {
		return "", fmt.Errorf("path traversal attempts not allowed")
	}

	// Check for empty or current directory references
	if cleanName == "." || cleanName == "" {
		return "", fmt.Errorf("invalid path: empty or current directory reference")
	}

	// Join with target directory
	targetPath := filepath.Join(targetDir, cleanName)

	// Ensure the resolved path is still within the target directory
	absTargetDir, err := filepath.Abs(targetDir)
	if err != nil {
		return "", fmt.Errorf("failed to resolve target directory: %w", err)
	}

	absTargetPath, err := filepath.Abs(targetPath)
	if err != nil {
		return "", fmt.Errorf("failed to resolve target path: %w", err)
	}

	// Check if the target path is within the target directory
	relPath, err := filepath.Rel(absTargetDir, absTargetPath)
	if err != nil {
		return "", fmt.Errorf("failed to compute relative path: %w", err)
	}

	// If the relative path starts with "..", it's outside the target directory
	if strings.HasPrefix(relPath, "..") {
		return "", fmt.Errorf("path escapes target directory")
	}

	// Additional security checks
	if len(cleanName) > 255 {
		return "", fmt.Errorf("path too long (max 255 characters)")
	}

	// Check for suspicious characters that might be used in attacks
	suspiciousChars := []string{"\x00", "\r", "\n"}
	for _, char := range suspiciousChars {
		if strings.Contains(cleanName, char) {
			return "", fmt.Errorf("path contains suspicious characters")
		}
	}

	return targetPath, nil
}

// isWindowsAbsolutePath checks if a path is a Windows-style absolute path
func isWindowsAbsolutePath(path string) bool {
	// Check for Windows drive letter patterns like C:\ or C:/
	if len(path) >= 3 && path[1] == ':' && (path[2] == '\\' || path[2] == '/') {
		// Check if first character is a letter
		c := path[0]
		return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')
	}

	// Check for UNC paths like \\server\share
	if len(path) >= 2 && path[0] == '\\' && path[1] == '\\' {
		return true
	}

	return false
}
