package cli

import (
	"archive/tar"
	"compress/gzip"
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
	// Determine project directory
	projectDir := "."
	if len(args) > 0 {
		projectDir = args[0]
	}

	// Make path absolute
	absProjectDir, err := filepath.Abs(projectDir)
	if err != nil {
		return fmt.Errorf("❌ Failed to resolve project directory: %w", err)
	}

	// Find and load configuration
	// Change to project directory temporarily to find config
	originalDir, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("❌ Failed to get current directory: %w", err)
	}

	if err := os.Chdir(absProjectDir); err != nil {
		return fmt.Errorf("❌ Failed to change to project directory: %w", err)
	}

	configPath, cfg, err := config.FindConfig()

	// Change back to original directory
	if chErr := os.Chdir(originalDir); chErr != nil {
		// Log the error but don't fail the operation
		fmt.Printf("Warning: Failed to change back to original directory: %v\n", chErr)
	}

	if err != nil {
		return fmt.Errorf("❌ Failed to find Icebox configuration in %s\n"+
			"💡 Try running 'icebox init' first to create a new project: %w", absProjectDir, err)
	}

	if cmd.Flag("verbose").Value.String() == "true" {
		fmt.Printf("Packing project: %s\n", absProjectDir)
		fmt.Printf("Using configuration: %s\n", configPath)
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

	// Create the archive
	if err := createArchive(absProjectDir, outputPath, cfg); err != nil {
		return fmt.Errorf("❌ Failed to create archive: %w", err)
	}

	// Display success message
	fileInfo, _ := os.Stat(outputPath)
	fmt.Printf("✅ Successfully created archive!\n\n")
	fmt.Printf("📦 Archive Details:\n")
	fmt.Printf("   File: %s\n", outputPath)
	if fileInfo != nil {
		fmt.Printf("   Size: %s\n", formatBytes(fileInfo.Size()))
	}
	fmt.Printf("   Includes data: %v\n", packOpts.includeData)
	fmt.Printf("   Checksums: %v\n", packOpts.checksum)

	return nil
}

func runUnpack(cmd *cobra.Command, args []string) error {
	archivePath := args[0]

	// Determine target directory
	targetDir := unpackOpts.targetDir
	if targetDir == "" {
		targetDir = "."
	}

	// Make path absolute
	absTargetDir, err := filepath.Abs(targetDir)
	if err != nil {
		return fmt.Errorf("❌ Failed to resolve target directory: %w", err)
	}

	// Extract the archive
	if err := extractArchive(archivePath, absTargetDir); err != nil {
		return fmt.Errorf("❌ Failed to extract archive: %w", err)
	}

	// Display success message
	fmt.Printf("✅ Successfully extracted archive!\n\n")
	fmt.Printf("📂 Extraction Details:\n")
	fmt.Printf("   Archive: %s\n", archivePath)
	fmt.Printf("   Target: %s\n", absTargetDir)
	fmt.Printf("   Verified: %v\n", unpackOpts.verify)

	return nil
}

func createArchive(projectDir, outputPath string, cfg *config.Config) error {
	// Create output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
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

	// Walk the project directory
	err = filepath.Walk(projectDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip if it's the output file itself
		if path == outputPath {
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(projectDir, path)
		if err != nil {
			return err
		}

		// Skip certain files/directories
		if shouldSkip(relPath, info) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		// Skip data files if not including data
		if !packOpts.includeData && isDataFile(relPath) {
			return nil
		}

		// Check size limits
		if totalSize+info.Size() > packOpts.maxSize {
			return fmt.Errorf("archive would exceed maximum size limit (%s)", formatBytes(packOpts.maxSize))
		}

		// Add file to archive
		if !info.IsDir() {
			if err := addFileToArchive(tarWriter, path, relPath, info, manifest); err != nil {
				return fmt.Errorf("failed to add file %s: %w", relPath, err)
			}
			totalSize += info.Size()
			fileCount++
		} else {
			// Add directory entry
			if err := addDirToArchive(tarWriter, relPath, info); err != nil {
				return fmt.Errorf("failed to add directory %s: %w", relPath, err)
			}
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk project directory: %w", err)
	}

	// Update manifest totals
	manifest.PackageInfo.TotalSize = totalSize
	manifest.PackageInfo.FileCount = fileCount

	// Add manifest to archive
	if err := addManifestToArchive(tarWriter, manifest); err != nil {
		return fmt.Errorf("failed to add manifest: %w", err)
	}

	fmt.Printf("📦 Packed %d files (%s total)\n", fileCount, formatBytes(totalSize))

	return nil
}

func extractArchive(archivePath, targetDir string) error {
	// Open archive file
	archiveFile, err := os.Open(archivePath)
	if err != nil {
		return fmt.Errorf("failed to open archive: %w", err)
	}
	defer archiveFile.Close()

	// Create reader chain
	var reader io.Reader = archiveFile

	// Check if it's gzipped
	if strings.HasSuffix(archivePath, ".gz") {
		gzipReader, err := gzip.NewReader(archiveFile)
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}
		defer gzipReader.Close()
		reader = gzipReader
	}

	tarReader := tar.NewReader(reader)

	var manifest *PackageManifest
	extractedFiles := 0

	// Extract files
	for {
		header, err := tarReader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		targetPath := filepath.Join(targetDir, header.Name)

		// Handle manifest
		if header.Name == "manifest.json" {
			manifestData, err := io.ReadAll(tarReader)
			if err != nil {
				return fmt.Errorf("failed to read manifest: %w", err)
			}
			if err := json.Unmarshal(manifestData, &manifest); err != nil {
				return fmt.Errorf("failed to parse manifest: %w", err)
			}
			continue
		}

		// Skip data files if requested
		if unpackOpts.skipData && isDataFile(header.Name) {
			continue
		}

		// Check if file exists and handle overwrite
		if _, err := os.Stat(targetPath); err == nil && !unpackOpts.overwrite {
			fmt.Printf("⚠️  Skipping existing file: %s (use --overwrite to replace)\n", header.Name)
			continue
		}

		// Create target directory
		if err := os.MkdirAll(filepath.Dir(targetPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}

		// Extract based on type
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return fmt.Errorf("failed to create directory %s: %w", targetPath, err)
			}

		case tar.TypeReg:
			if err := extractFile(tarReader, targetPath, header, manifest); err != nil {
				return fmt.Errorf("failed to extract file %s: %w", header.Name, err)
			}
			extractedFiles++
		}
	}

	fmt.Printf("📂 Extracted %d files\n", extractedFiles)

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
