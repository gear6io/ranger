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
	ExcludePaths               []string `yaml:"exclude_paths"`
	ForbiddenPatterns          []string `yaml:"forbidden_patterns"`
	CheckForbidden             bool     `yaml:"check_forbidden"`
	CheckInternal              bool     `yaml:"check_internal"`
	CheckInternalPackageCalls  bool     `yaml:"check_internal_package_calls"`
	OutputFormat               string   `yaml:"output_format"`
	ExitOnUnused               bool     `yaml:"exit_on_unused"`
	ExitOnForbidden            bool     `yaml:"exit_on_forbidden"`
	ExitOnInternal             bool     `yaml:"exit_on_internal"`
	ExitOnInternalPackageCalls bool     `yaml:"exit_on_internal_package_calls"`
	Verbose                    bool     `yaml:"verbose"`
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

// FunctionInfo represents information about a function
type FunctionInfo struct {
	Name           string
	File           string
	Line           int
	Package        string
	Exported       bool
	UsesErrorsNew  bool
	ErrorsNewLines []int
	// Track function calls to internal packages that result in errors.New usage
	InternalPackageCalls map[string][]InternalCallInfo
}

// InternalCallInfo represents information about a call to an internal package
type InternalCallInfo struct {
	Package   string
	Function  string
	Line      int
	ErrorLine int // Line where errors.New is used to handle this call's error
}

// ErrorCodeChecker checks for proper ErrorCode usage
type ErrorCodeChecker struct {
	fileSet      *token.FileSet
	errorCodes   map[string]*ErrorCodeInfo
	functions    map[string]*FunctionInfo
	excludePaths []string
	verbose      bool
}

// NewErrorCodeChecker creates a new ErrorCodeChecker
func NewErrorCodeChecker(verbose bool) *ErrorCodeChecker {
	return &ErrorCodeChecker{
		errorCodes:   make(map[string]*ErrorCodeInfo),
		functions:    make(map[string]*FunctionInfo),
		fileSet:      token.NewFileSet(),
		excludePaths: []string{}, // Initialize excludePaths
		verbose:      verbose,
	}
}

// debug prints debug output only when verbose mode is enabled
func (c *ErrorCodeChecker) debug(format string, args ...interface{}) {
	if c.verbose {
		fmt.Printf(format, args...)
	}
}

// CheckDirectory recursively checks a directory for ErrorCode usage
func (c *ErrorCodeChecker) CheckDirectory(dir string, excludePaths []string) error {
	// Store exclude paths for use in checking methods
	c.excludePaths = excludePaths

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

	if serverIndex != -1 && serverIndex+1 < len(parts) {
		// Get the package path after "server/"
		packageParts := parts[serverIndex+1:]
		if len(packageParts) > 1 {
			// Remove the filename and join the directory parts
			return strings.Join(packageParts[:len(packageParts)-1], "/")
		}
	}

	// For test files or other cases, try to extract from the last directory
	if len(parts) >= 2 {
		// Use the second-to-last part as package name (last part is filename)
		return parts[len(parts)-2]
	}

	return "unknown"
}

// CheckFile checks a single Go file for ErrorCode usage
func (c *ErrorCodeChecker) CheckFile(filePath string) error {
	file, err := parser.ParseFile(c.fileSet, filePath, nil, parser.ParseComments)
	if err != nil {
		return fmt.Errorf("failed to parse file %s: %w", filePath, err)
	}

	// Check for function declarations
	c.checkFunctionDeclarations(file, filePath)

	// Check for internal package calls
	c.checkInternalPackageCalls(file, filePath)

	// Check for ErrorCode declarations
	c.checkDeclarations(file, filePath)

	// Check for ErrorCode usage and errors.New usage
	c.checkUsage(file, filePath)

	return nil
}

// checkFunctionDeclarations finds function declarations and tracks their export status
func (c *ErrorCodeChecker) checkFunctionDeclarations(file *ast.File, filePath string) {
	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			if x.Name != nil {
				pos := c.fileSet.Position(x.Name.Pos())
				packageName := c.extractPackageName(filePath)
				exported := x.Name.IsExported()

				// Create unique key for function
				funcKey := fmt.Sprintf("%s:%s:%s", packageName, filepath.Base(filePath), x.Name.Name)

				c.functions[funcKey] = &FunctionInfo{
					Name:                 x.Name.Name,
					File:                 filePath,
					Line:                 pos.Line,
					Package:              packageName,
					Exported:             exported,
					UsesErrorsNew:        false,
					ErrorsNewLines:       []int{},
					InternalPackageCalls: make(map[string][]InternalCallInfo),
				}

			}
		}
		return true
	})
}

// checkInternalPackageCalls finds calls to internal packages and tracks them
func (c *ErrorCodeChecker) checkInternalPackageCalls(file *ast.File, filePath string) {
	// Check if this file path should be excluded
	for _, excludePath := range c.excludePaths {
		if strings.Contains(filePath, excludePath) {
			return // Skip this file if it's in an excluded path
		}
	}

	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			// Check for function calls and method calls
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				// Debug: print what we're looking at
				c.debug("DEBUG: Found selector: %+v\n", sel)

				// Check if this is a call to an internal package
				if c.isInternalPackageCall(sel, filePath) {
					pos := c.fileSet.Position(x.Pos())
					currentPackage := c.extractPackageName(filePath)

					// Find the containing function
					funcKey := c.findContainingFunction(file, pos, currentPackage, filePath)
					if funcKey != "" {
						if funcInfo, exists := c.functions[funcKey]; exists {
							// Track this internal package call
							callInfo := c.extractCallInfo(sel, pos.Line)

							if funcInfo.InternalPackageCalls == nil {
								funcInfo.InternalPackageCalls = make(map[string][]InternalCallInfo)
							}

							packageKey := callInfo.Package + "." + callInfo.Function
							funcInfo.InternalPackageCalls[packageKey] = append(funcInfo.InternalPackageCalls[packageKey], callInfo)
						}
					}
				}
			}
		}
		return true
	})
}

// isInternalPackageCall checks if a selector expression refers to an internal package call
func (c *ErrorCodeChecker) isInternalPackageCall(sel *ast.SelectorExpr, filePath string) bool {
	// Debug: print what we're checking
	c.debug("DEBUG: isInternalPackageCall checking selector: %+v\n", sel)

	// Check different patterns of internal package calls

	// Pattern 1: catalog.Parse() - direct package function call
	// OR catalogManager.LoadTable() - type method call
	if ident, ok := sel.X.(*ast.Ident); ok {
		c.debug("DEBUG: Pattern 1 - identifier: %s\n", ident.Name)

		// Check if this is a direct package call (like catalog.Parse)
		if c.isInternalPackage(ident.Name, filePath) {
			c.debug("DEBUG: Pattern 1a - direct package call: %s\n", ident.Name)
			return true
		}

		// Check if this is a type method call (like catalogManager.LoadTable)
		if c.isInternalPackageVariable(ident.Name) {
			c.debug("DEBUG: Pattern 1b - type method call: %s\n", ident.Name)
			return true
		}

		return false
	}

	// Pattern 2: catalogManager.LoadTable() - type method call
	if sel, ok := sel.X.(*ast.SelectorExpr); ok {
		c.debug("DEBUG: Pattern 2 - nested selector found\n")
		if ident, ok := sel.X.(*ast.Ident); ok {
			c.debug("DEBUG: Pattern 2 - variable name: %s\n", ident.Name)
			// Check if this looks like an internal package variable
			// Common patterns: catalogManager, storageEngine, queryParser, etc.
			return c.isInternalPackageVariable(ident.Name)
		}
	}

	// Pattern 3: catalogManager.GetNamespace().GetTable() - chained method calls
	// We need to traverse up the chain to find the root package
	c.debug("DEBUG: Pattern 3 - checking chained calls\n")
	return c.isChainedInternalPackageCall(sel, filePath)
}

// isInternalPackageVariable checks if a variable name suggests it's from an internal package
func (c *ErrorCodeChecker) isInternalPackageVariable(varName string) bool {
	// Common prefixes for internal package variables
	internalPrefixes := []string{
		"catalog", "storage", "query", "metadata", "paths", "loader", "gateway", "astha",
		"config", "protocols", "shared", "types", "parser", "duckdb", "memory", "filesystem",
		"iceberg", "registry", "scheduler", "event_store", "cdc_consumer",
		"errors", "sdk", "client", "utils", "cmd",
	}

	// Debug: print what we're checking
	c.debug("DEBUG: Checking variable '%s' for internal package prefixes\n", varName)

	for _, prefix := range internalPrefixes {
		if strings.HasPrefix(varName, prefix) {
			c.debug("DEBUG: Variable '%s' matches prefix '%s'\n", varName, prefix)
			return true
		}
	}
	c.debug("DEBUG: Variable '%s' does not match any internal package prefix\n", varName)
	return false
}

// extractPackageFromVariable extracts the package name from a variable name
func (c *ErrorCodeChecker) extractPackageFromVariable(varName string) string {
	// Common prefixes for internal package variables
	internalPrefixes := []string{
		"catalog", "storage", "query", "metadata", "paths", "loader", "gateway", "astha",
		"config", "protocols", "shared", "types", "parser", "duckdb", "memory", "filesystem",
		"iceberg", "registry", "scheduler", "event_store", "cdc_consumer",
		"errors", "sdk", "client", "utils", "cmd",
	}

	for _, prefix := range internalPrefixes {
		if strings.HasPrefix(varName, prefix) {
			return prefix
		}
	}
	return "unknown"
}

// isChainedInternalPackageCall checks if a chained method call originates from an internal package
func (c *ErrorCodeChecker) isChainedInternalPackageCall(sel *ast.SelectorExpr, filePath string) bool {
	// Traverse up the chain to find the root
	current := sel
	for {
		if ident, ok := current.X.(*ast.Ident); ok {
			// Found the root identifier, check if it's an internal package
			return c.isInternalPackage(ident.Name, filePath) || c.isInternalPackageVariable(ident.Name)
		}

		if sel, ok := current.X.(*ast.SelectorExpr); ok {
			current = sel
		} else {
			// Can't traverse further
			break
		}
	}
	return false
}

// extractCallInfo extracts call information from a selector expression
func (c *ErrorCodeChecker) extractCallInfo(sel *ast.SelectorExpr, line int) InternalCallInfo {
	// Debug: print what we're extracting
	c.debug("DEBUG: extractCallInfo called with selector: %+v\n", sel)

	// For now, extract the immediate package and function
	// In a more sophisticated implementation, we could extract the full chain

	if ident, ok := sel.X.(*ast.Ident); ok {
		// Direct package call: catalog.Parse()
		// OR Type method call: catalogManager.LoadTable()

		// Check if this is a type method call
		if c.isInternalPackageVariable(ident.Name) {
			// Type method call: catalogManager.LoadTable()
			packageName := c.extractPackageFromVariable(ident.Name)
			c.debug("DEBUG: Type method call - variable: %s, package: %s, method: %s\n", ident.Name, packageName, sel.Sel.Name)
			return InternalCallInfo{
				Package:   packageName,
				Function:  sel.Sel.Name,
				Line:      line,
				ErrorLine: 0,
			}
		} else {
			// Direct package call: catalog.Parse()
			c.debug("DEBUG: Direct package call - package: %s, function: %s\n", ident.Name, sel.Sel.Name)
			return InternalCallInfo{
				Package:   ident.Name,
				Function:  sel.Sel.Name,
				Line:      line,
				ErrorLine: 0,
			}
		}
	}

	if sel, ok := sel.X.(*ast.SelectorExpr); ok {
		if ident, ok := sel.X.(*ast.Ident); ok {
			// Type method call: catalogManager.LoadTable()
			// Extract the package name from the variable name
			packageName := c.extractPackageFromVariable(ident.Name)
			c.debug("DEBUG: Nested selector - variable: %s, package: %s, method: %s\n", ident.Name, packageName, sel.Sel.Name)
			return InternalCallInfo{
				Package:   packageName,
				Function:  sel.Sel.Name, // Just the method name
				Line:      line,
				ErrorLine: 0,
			}
		}
	}

	// Fallback
	c.debug("DEBUG: Fallback - method: %s\n", sel.Sel.Name)
	return InternalCallInfo{
		Package:   "unknown",
		Function:  sel.Sel.Name,
		Line:      line,
		ErrorLine: 0,
	}
}

// isInternalPackage checks if a package name refers to an internal gear6io/ranger package
func (c *ErrorCodeChecker) isInternalPackage(packageName string, currentFile string) bool {
	// Check if this is a call to a package within the github.com/gear6io/ranger project
	// We need to look at the imports to determine this
	// For now, let's check if the package name matches common project package patterns

	// Common project package names that would be internal to gear6io/ranger
	internalPackages := []string{
		// Server packages
		"catalog", "storage", "query", "metadata", "paths", "loader", "gateway", "astha",
		"config", "protocols", "shared", "types", "parser", "duckdb", "memory", "filesystem",
		"iceberg", "registry", "scheduler", "event_store", "cdc_consumer",

		// Other project packages
		"errors", "sdk", "client", "utils", "cmd",
	}

	for _, pkg := range internalPackages {
		if packageName == pkg {
			return true
		}
	}

	return false
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

// checkUsage finds where ErrorCode variables are used and tracks errors.New usage in internal functions
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
		case *ast.CallExpr:
			// Check for errors.New usage
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if ident, ok := sel.X.(*ast.Ident); ok {
					if ident.Name == "errors" && sel.Sel.Name == "New" {
						// Found errors.New usage, now find which function contains it
						pos := c.fileSet.Position(x.Pos())
						currentPackage := c.extractPackageName(filePath)

						// Find the containing function
						funcKey := c.findContainingFunction(file, pos, currentPackage, filePath)
						if funcKey != "" {
							if funcInfo, exists := c.functions[funcKey]; exists {
								funcInfo.UsesErrorsNew = true
								if !containsInt(funcInfo.ErrorsNewLines, pos.Line) {
									funcInfo.ErrorsNewLines = append(funcInfo.ErrorsNewLines, pos.Line)
								}

								// Check if this errors.New is handling an error from an internal package call
								c.linkErrorsNewToInternalCalls(funcInfo, pos.Line, filePath)
							}
						}
					}
				}
			}
		case *ast.IfStmt:
			// Look for error handling patterns: if err != nil
			if cond, ok := x.Cond.(*ast.BinaryExpr); ok {
				if ident, ok := cond.X.(*ast.Ident); ok {
					if ident.Name == "err" && cond.Op == token.NEQ {
						if nilIdent, ok := cond.Y.(*ast.Ident); ok && nilIdent.Name == "nil" {
							// Found "if err != nil" pattern
							// Now look for errors.New in the body
							c.checkErrorHandlingBody(x.Body, filePath)
						}
					}
				}
			}
		}
		return true
	})
}

// checkErrorHandlingBody checks the body of an error handling if statement for errors.New usage
func (c *ErrorCodeChecker) checkErrorHandlingBody(body *ast.BlockStmt, filePath string) {
	ast.Inspect(body, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if sel, ok := x.Fun.(*ast.SelectorExpr); ok {
				if ident, ok := sel.X.(*ast.Ident); ok {
					if ident.Name == "errors" && sel.Sel.Name == "New" {
						// Found errors.New in error handling context
						pos := c.fileSet.Position(x.Pos())

						// Find the containing function by searching through all functions
						// and finding the one that contains this position
						for funcKey, funcInfo := range c.functions {
							if strings.Contains(funcKey, filepath.Base(filePath)) {
								// Check if this position is within the function
								if pos.Line >= funcInfo.Line && pos.Line <= funcInfo.Line+100 { // Rough estimate
									// This errors.New is definitely handling an error
									// Link it to the most recent internal package call
									c.linkErrorsNewToInternalCalls(funcInfo, pos.Line, filePath)
									break
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

// linkErrorsNewToInternalCalls links errors.New usage to internal package calls
func (c *ErrorCodeChecker) linkErrorsNewToInternalCalls(funcInfo *FunctionInfo, errorLine int, filePath string) {
	// Look for recent internal package calls in this function
	// We need to be more precise about linking calls to errors.New usage

	for packageKey, calls := range funcInfo.InternalPackageCalls {
		for i, call := range calls {
			// Only link if this call is before the errors.New and reasonably close
			// Also check if there's an error handling pattern (if err != nil)
			if call.Line < errorLine && (errorLine-call.Line) <= 15 {
				// Update the call info with the error line
				funcInfo.InternalPackageCalls[packageKey][i].ErrorLine = errorLine
			}
		}
	}
}

// findContainingFunction finds which function contains a given position
func (c *ErrorCodeChecker) findContainingFunction(file *ast.File, pos token.Position, packageName string, filePath string) string {
	var currentFunc string

	ast.Inspect(file, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.FuncDecl:
			if x.Name != nil {
				funcStart := c.fileSet.Position(x.Pos())
				funcEnd := c.fileSet.Position(x.End())

				// Check if the position is within this function
				if pos.Line >= funcStart.Line && pos.Line <= funcEnd.Line {
					// Use the same key format as in checkFunctionDeclarations
					// The key format is: packageName:filename:functionName
					currentFunc = fmt.Sprintf("%s:%s:%s", packageName, filepath.Base(filePath), x.Name.Name)
				}
			}
		}
		return true
	})

	return currentFunc
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

// containsInt checks if a slice of integers contains an integer
func containsInt(slice []int, item int) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Report generates a report of unused ErrorCodes and internal functions using errors.New
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

// ReportInternalErrorsNewUsage reports internal functions that use errors.New
func (c *ErrorCodeChecker) ReportInternalErrorsNewUsage() (bool, []string) {
	var violations []string
	var report []string

	// Group functions by package
	packageGroups := make(map[string][]*FunctionInfo)
	for _, info := range c.functions {
		if !info.Exported && info.UsesErrorsNew {
			packageGroups[info.Package] = append(packageGroups[info.Package], info)
		}
	}

	if len(packageGroups) == 0 {
		return true, []string{"✅ No internal functions using errors.New found"}
	}

	// Sort packages for consistent output
	var packages []string
	for pkg := range packageGroups {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)

	report = append(report, "\n🚨 INTERNAL FUNCTIONS USING errors.New (should use ErrorCodes):")

	// Generate report grouped by package
	for _, pkg := range packages {
		report = append(report, fmt.Sprintf("\n📦 Package: %s", pkg))

		infos := packageGroups[pkg]
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Name < infos[j].Name
		})

		for _, info := range infos {
			violations = append(violations, fmt.Sprintf("%s:%s", info.File, info.Name))
			linesStr := strings.Trim(strings.Replace(fmt.Sprint(info.ErrorsNewLines), " ", ",", -1), "[]")
			report = append(report, fmt.Sprintf("  ❌ INTERNAL: %s in %s:%d (errors.New at lines: %s)",
				info.Name, info.File, info.Line, linesStr))
		}
	}

	return len(violations) == 0, report
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

// ReportInternalPackageErrorsNewUsage reports when errors.New is used to handle errors from internal package calls
func (c *ErrorCodeChecker) ReportInternalPackageErrorsNewUsage() (bool, []string) {
	var violations []string
	var report []string

	// Group functions by package
	packageGroups := make(map[string][]*FunctionInfo)
	for _, info := range c.functions {
		hasInternalPackageErrors := false
		for _, calls := range info.InternalPackageCalls {
			for _, call := range calls {
				if call.ErrorLine > 0 {
					hasInternalPackageErrors = true
					break
				}
			}
			if hasInternalPackageErrors {
				break
			}
		}

		if hasInternalPackageErrors {
			packageGroups[info.Package] = append(packageGroups[info.Package], info)
		}
	}

	if len(packageGroups) == 0 {
		return true, []string{"✅ No errors.New usage for internal package calls found"}
	}

	// Sort packages for consistent output
	var packages []string
	for pkg := range packageGroups {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)

	report = append(report, "\n🚨 ERRORS.NEW USED FOR INTERNAL PACKAGE CALLS (should use ErrorCodes):")

	// Generate report grouped by package
	for _, pkg := range packages {
		report = append(report, fmt.Sprintf("\n📦 Package: %s", pkg))

		infos := packageGroups[pkg]
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Name < infos[j].Name
		})

		for _, info := range infos {
			for _, calls := range info.InternalPackageCalls {
				for _, call := range calls {
					if call.ErrorLine > 0 {
						violations = append(violations, fmt.Sprintf("%s:%s", info.File, info.Name))
						report = append(report, fmt.Sprintf("  ❌ INTERNAL_CALL: %s in %s:%d calls %s.%s at line %d, uses errors.New at line %d",
							info.Name, info.File, info.Line, call.Package, call.Function, call.Line, call.ErrorLine))
					}
				}
			}
		}
	}

	return len(violations) == 0, report
}

// loadConfig loads configuration from file or uses defaults
func loadConfig(configPath string) (*Config, error) {
	config := &Config{
		ExcludePaths:               []string{"pkg/sdk/", "integration_tests/", "testdata/", "milestones/", "scripts/", "demo/", "data/", "logs/", "workspace/", "my-lakehouse/", "vendor/", "node_modules/", ".git/"},
		ForbiddenPatterns:          []string{`fmt\.Errorf`, `errors\.New\(`, `errors\.Wrap\(`, `errors\.Wrapf\(`},
		CheckForbidden:             true,
		CheckInternal:              true,
		CheckInternalPackageCalls:  true,
		OutputFormat:               "human",
		ExitOnUnused:               true,
		ExitOnForbidden:            true,
		ExitOnInternal:             true,
		ExitOnInternalPackageCalls: true,
		Verbose:                    false,
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

	checker := NewErrorCodeChecker(config.Verbose)

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

	// Track all violations for comprehensive reporting
	var hasAnyViolations bool

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
			hasAnyViolations = true
		}
	}

	// Report internal functions using errors.New if requested
	if config.CheckInternal {
		fmt.Println("🔍 Checking for internal functions using errors.New...")
		noViolations, internalReport := checker.ReportInternalErrorsNewUsage()

		for _, line := range internalReport {
			fmt.Println(line)
		}
		fmt.Println()

		if !noViolations {
			fmt.Println("❌ Found internal functions using errors.New!")
			hasAnyViolations = true
		}
	}

	// Report internal package calls using errors.New if requested
	if config.CheckInternalPackageCalls {
		fmt.Println("🔍 Checking for errors.New usage for internal package calls...")
		noViolations, internalPackageReport := checker.ReportInternalPackageErrorsNewUsage()

		for _, line := range internalPackageReport {
			fmt.Println(line)
		}
		fmt.Println()

		if !noViolations {
			fmt.Println("❌ Found errors.New usage for internal package calls!")
			hasAnyViolations = true
		}
	}

	// Final summary
	fmt.Println("📊 FINAL SUMMARY:")
	fmt.Println("==================")

	if allUsed {
		fmt.Println("✅ All ErrorCodes are being used!")
	} else {
		fmt.Println("❌ Found unused ErrorCodes!")
	}

	if hasAnyViolations {
		fmt.Println("❌ Found linting violations!")
	} else {
		fmt.Println("✅ No linting violations found!")
	}

	// Exit with appropriate code based on configuration and violations
	if !allUsed && config.ExitOnUnused {
		fmt.Println("🚨 Exiting due to unused ErrorCodes (exit_on_unused: true)")
		os.Exit(1)
	}

	if hasAnyViolations {
		if config.ExitOnForbidden || config.ExitOnInternal || config.ExitOnInternalPackageCalls {
			fmt.Println("🚨 Exiting due to linting violations (exit_on_*: true)")
			os.Exit(1)
		}
	}

	fmt.Println("✅ All checks completed successfully!")
	os.Exit(0)
}
