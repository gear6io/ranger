package main

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config represents the ErrorCode checker configuration
type Config struct {
	ExcludePaths      []string `yaml:"exclude_paths"`
	ForbiddenPatterns []string `yaml:"forbidden_patterns"`
	CheckForbidden    bool     `yaml:"check_forbidden"`
	OutputFormat      string   `yaml:"output_format"`
	ExitOnUnused      bool     `yaml:"exit_on_unused"`
	ExitOnForbidden   bool     `yaml:"exit_on_forbidden"`
	Verbose           bool     `yaml:"verbose"`
}

// ErrorCodeInfo represents information about an ErrorCode variable
type ErrorCodeInfo struct {
	Name     string
	File     string
	Line     int
	Package  string
	Used     bool
	UsedIn   []string
	Declared bool
}

// ErrorCodeChecker checks for unused ErrorCode variables
type ErrorCodeChecker struct {
	errorCodes map[string]*ErrorCodeInfo
	fileSet    *token.FileSet
}

// NewErrorCodeChecker creates a new ErrorCodeChecker
func NewErrorCodeChecker() *ErrorCodeChecker {
	return &ErrorCodeChecker{
		errorCodes: make(map[string]*ErrorCodeInfo),
		fileSet:    token.NewFileSet(),
	}
}

// CheckDirectory recursively checks a directory for ErrorCode usage
func (c *ErrorCodeChecker) CheckDirectory(dir string, excludePaths []string) error {
	return filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip excluded paths
		for _, excludePath := range excludePaths {
			if strings.Contains(path, excludePath) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		// Skip non-Go files
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}

		return c.CheckFile(path)
	})
}

// extractPackageName extracts the package name from a file path
func (c *ErrorCodeChecker) extractPackageName(filePath string) string {
	// Extract package from path like "../../../server/storage/memory/errors.go"
	// We want to get "storage/memory" as the package
	parts := strings.Split(filePath, "/")

	// Find the "server" directory index
	serverIndex := -1
	for i, part := range parts {
		if part == "server" {
			serverIndex = i
			break
		}
	}

	if serverIndex == -1 || serverIndex+1 >= len(parts) {
		return "unknown"
	}

	// Get the package path after "server/"
	packageParts := parts[serverIndex+1:]
	if len(packageParts) > 1 {
		// Remove the filename and join the directory parts
		return strings.Join(packageParts[:len(packageParts)-1], "/")
	}

	return "unknown"
}

// CheckFile checks a single Go file for ErrorCode usage
func (c *ErrorCodeChecker) CheckFile(filePath string) error {
	file, err := parser.ParseFile(c.fileSet, filePath, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("failed to parse file %s: %w", filePath, err)
	}

	// Check for ErrorCode declarations
	c.checkDeclarations(file, filePath)

	// Check for ErrorCode usage
	c.checkUsage(file, filePath)

	return nil
}

// checkDeclarations finds ErrorCode variable declarations
func (c *ErrorCodeChecker) checkDeclarations(file *ast.File, filePath string) {
	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.ValueSpec:
			for i, name := range x.Names {
				if strings.HasPrefix(name.Name, "Err") {
					// Check if this is an ErrorCode declaration
					if len(x.Values) > i {
						if call, ok := x.Values[i].(*ast.CallExpr); ok {
							if sel, ok := call.Fun.(*ast.SelectorExpr); ok {
								if ident, ok := sel.X.(*ast.Ident); ok {
									if ident.Name == "errors" && sel.Sel.Name == "MustNewCode" {
										// This is an ErrorCode declaration
										pos := c.fileSet.Position(name.Pos())
										// Extract package name from file path
										packageName := c.extractPackageName(filePath)
										c.errorCodes[name.Name] = &ErrorCodeInfo{
											Name:     name.Name,
											File:     filePath,
											Line:     pos.Line,
											Package:  packageName,
											Declared: true,
											Used:     false,
											UsedIn:   []string{},
										}
									}
								}
							}
						}
					}
				}
			}
		}
		return true
	})
}

// checkUsage finds where ErrorCode variables are used
func (c *ErrorCodeChecker) checkUsage(file *ast.File, filePath string) {
	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.Ident:
			if strings.HasPrefix(x.Name, "Err") {
				if info, exists := c.errorCodes[x.Name]; exists {
					// Skip if this is in the same file and line as the declaration
					pos := c.fileSet.Position(x.Pos())
					declarationPos := fmt.Sprintf("%s:%d", info.File, info.Line)
					currentPos := fmt.Sprintf("%s:%d", filePath, pos.Line)

					if declarationPos == currentPos {
						// This is the declaration, not usage
						return true
					}

					// Only mark as used if it's used within the same package
					currentPackage := c.extractPackageName(filePath)
					if info.Package == currentPackage {
						info.Used = true
						usage := currentPos
						if !contains(info.UsedIn, usage) {
							info.UsedIn = append(info.UsedIn, usage)
						}
					}
				}
			}
		}
		return true
	})
}

// contains checks if a slice contains a string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Report generates a report of unused ErrorCodes
func (c *ErrorCodeChecker) Report() (bool, []string) {
	var unused []string
	var report []string

	// Group ErrorCodes by package
	packageGroups := make(map[string][]*ErrorCodeInfo)
	for _, info := range c.errorCodes {
		packageGroups[info.Package] = append(packageGroups[info.Package], info)
	}

	// Sort packages for consistent output
	var packages []string
	for pkg := range packageGroups {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)

	// Generate report grouped by package
	for _, pkg := range packages {
		report = append(report, fmt.Sprintf("\n📦 Package: %s", pkg))

		infos := packageGroups[pkg]
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Name < infos[j].Name
		})

		for _, info := range infos {
			if !info.Used {
				unused = append(unused, info.Name)
				report = append(report, fmt.Sprintf("  ❌ UNUSED: %s declared in %s:%d", info.Name, info.File, info.Line))
			}
		}
	}

	return len(unused) == 0, report
}

// CheckForbiddenPatterns checks for forbidden error creation patterns
func (c *ErrorCodeChecker) CheckForbiddenPatterns(dir string, excludePaths []string, forbiddenPatterns []string) (bool, []string) {
	var violations []string
	var report []string

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip excluded paths
		for _, excludePath := range excludePaths {
			if strings.Contains(path, excludePath) {
				if info.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
		}

		// Skip non-Go files
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}

		// Read file content
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		// Check for forbidden patterns
		for _, pattern := range forbiddenPatterns {
			re := regexp.MustCompile(pattern)
			matches := re.FindAllStringIndex(string(content), -1)
			for _, match := range matches {
				line := strings.Count(string(content[:match[0]]), "\n") + 1
				violations = append(violations, fmt.Sprintf("%s:%d", path, line))
				report = append(report, fmt.Sprintf("❌ FORBIDDEN: %s in %s:%d", pattern, path, line))
			}
		}

		return nil
	})

	if err != nil {
		log.Printf("Error checking forbidden patterns: %v", err)
	}

	return len(violations) == 0, report
}

// loadConfig loads configuration from file or uses defaults
func loadConfig(configPath string) (*Config, error) {
	config := &Config{
		ExcludePaths:      []string{"pkg/sdk/", "integration_tests/", "testdata/", "milestones/", "scripts/", "demo/", "data/", "logs/", "workspace/", "my-lakehouse/", "vendor/", "node_modules/", ".git/"},
		ForbiddenPatterns: []string{`fmt\.Errorf`, `errors\.New\(`, `errors\.Wrap\(`, `errors\.Wrapf\(`},
		CheckForbidden:    true,
		OutputFormat:      "human",
		ExitOnUnused:      true,
		ExitOnForbidden:   true,
		Verbose:           false,
	}

	if configPath != "" {
		data, err := ioutil.ReadFile(configPath)
		if err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, fmt.Errorf("failed to parse config file: %w", err)
		}
	}

	return config, nil
}

func main() {
	var (
		dir        = flag.String("dir", ".", "Directory to check")
		configPath = flag.String("config", ".errorcode.yml", "Path to configuration file")
	)
	flag.Parse()

	// Load configuration
	config, err := loadConfig(*configPath)
	if err != nil {
		log.Printf("Warning: Using default configuration: %v", err)
		config, _ = loadConfig("")
	}

	checker := NewErrorCodeChecker()

	fmt.Printf("🔍 Checking ErrorCode usage in directory: %s\n", *dir)
	fmt.Printf("🚫 Excluding paths: %s\n", strings.Join(config.ExcludePaths, ", "))
	fmt.Printf("📋 Configuration: %s\n", *configPath)
	fmt.Println()

	// Check ErrorCode usage
	if err := checker.CheckDirectory(*dir, config.ExcludePaths); err != nil {
		log.Fatalf("Error checking directory: %v", err)
	}

	// Report ErrorCode usage
	allUsed, usageReport := checker.Report()
	for _, line := range usageReport {
		fmt.Println(line)
	}
	fmt.Println()

	// Check for forbidden patterns if requested
	if config.CheckForbidden {
		fmt.Println("🔍 Checking for forbidden error patterns...")
		noViolations, forbiddenReport := checker.CheckForbiddenPatterns(*dir, config.ExcludePaths, config.ForbiddenPatterns)

		for _, line := range forbiddenReport {
			fmt.Println(line)
		}
		fmt.Println()

		if !noViolations {
			fmt.Println("❌ Found forbidden error patterns!")
			if config.ExitOnForbidden {
				os.Exit(1)
			}
		}
	}

	// Final summary
	if allUsed {
		fmt.Println("✅ All ErrorCodes are being used!")
		os.Exit(0)
	} else {
		fmt.Println("❌ Found unused ErrorCodes!")
		if config.ExitOnUnused {
			os.Exit(1)
		}
	}
}
