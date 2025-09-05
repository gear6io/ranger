package main

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

// ErrorCodeChecker checks for proper ErrorCode usage
type ErrorCodeChecker struct {
	fileSet      *token.FileSet
	errorCodes   map[string]*ErrorCodeInfo
	functions    map[string]*FunctionInfo
	excludePaths []string
	verbose      bool
	analyzer     *SemanticAnalyzer
}

// NewErrorCodeChecker creates a new ErrorCodeChecker
func NewErrorCodeChecker(verbose bool) *ErrorCodeChecker {
	fileSet := token.NewFileSet()
	return &ErrorCodeChecker{
		errorCodes:   make(map[string]*ErrorCodeInfo),
		functions:    make(map[string]*FunctionInfo),
		fileSet:      fileSet,
		excludePaths: []string{}, // Initialize excludePaths
		verbose:      verbose,
		analyzer:     NewSemanticAnalyzer(fileSet),
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

	// Initialize semantic analyzer for this file
	c.analyzer.analyzeFile(file)

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

				// Check if this is a call to an internal package (but exclude errors package calls)
				if c.isInternalPackageCall(sel, filePath) && !c.isErrorsPackageCall(sel) {
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

// isErrorsPackageCall checks if a selector expression is a call to the errors package
func (c *ErrorCodeChecker) isErrorsPackageCall(sel *ast.SelectorExpr) bool {
	if ident, ok := sel.X.(*ast.Ident); ok {
		return ident.Name == "errors"
	}
	return false
}

// isInternalPackageCall checks if a selector expression refers to an internal package call
func (c *ErrorCodeChecker) isInternalPackageCall(sel *ast.SelectorExpr, filePath string) bool {
	// Use semantic analysis to determine if this is an internal package call
	return c.analyzer.isInternalPackageCall(sel)
}

// extractCallInfo extracts call information from a selector expression
func (c *ErrorCodeChecker) extractCallInfo(sel *ast.SelectorExpr, line int) InternalCallInfo {
	// Use semantic analysis to extract accurate call information
	if ident, ok := sel.X.(*ast.Ident); ok {
		// Check if it's a variable or parameter (method call)
		if varType, isVar := c.analyzer.variables[ident.Name]; isVar {
			return InternalCallInfo{
				Package:   c.extractPackageFromType(varType),
				Function:  sel.Sel.Name,
				Line:      line,
				ErrorLine: 0,
			}
		}
		if paramType, isParam := c.analyzer.parameters[ident.Name]; isParam {
			return InternalCallInfo{
				Package:   c.extractPackageFromType(paramType),
				Function:  sel.Sel.Name,
				Line:      line,
				ErrorLine: 0,
			}
		}

		// Check if it's a direct package call
		if importPath, isImport := c.analyzer.imports[ident.Name]; isImport {
			return InternalCallInfo{
				Package:   c.extractPackageFromImport(importPath),
				Function:  sel.Sel.Name,
				Line:      line,
				ErrorLine: 0,
			}
		}

		// Fallback to identifier name
		return InternalCallInfo{
			Package:   ident.Name,
			Function:  sel.Sel.Name,
			Line:      line,
			ErrorLine: 0,
		}
	}

	// Fallback
	return InternalCallInfo{
		Package:   "unknown",
		Function:  sel.Sel.Name,
		Line:      line,
		ErrorLine: 0,
	}
}

// extractPackageFromType extracts package name from a type string
func (c *ErrorCodeChecker) extractPackageFromType(typeName string) string {
	if strings.Contains(typeName, ".") {
		parts := strings.Split(typeName, ".")
		if len(parts) >= 2 {
			return parts[0]
		}
	}

	// For simple types, try to infer the package from common patterns
	if strings.Contains(typeName, "Manager") {
		return "manager"
	}
	if strings.Contains(typeName, "Engine") {
		return "engine"
	}
	if strings.Contains(typeName, "Parser") {
		return "parser"
	}

	return typeName
}

// extractPackageFromImport extracts package name from import path
func (c *ErrorCodeChecker) extractPackageFromImport(importPath string) string {
	parts := strings.Split(importPath, "/")
	if len(parts) > 0 {
		return parts[len(parts)-1]
	}
	return importPath
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

				// Analyze the error handling pattern to determine the appropriate approach
				c.analyzeErrorHandlingPattern(funcInfo, packageKey, i, errorLine, filePath)
			}
		}
	}
}

// analyzeErrorHandlingPattern analyzes the error handling pattern to determine if errors.New should be errors.AddContext or return err
func (c *ErrorCodeChecker) analyzeErrorHandlingPattern(funcInfo *FunctionInfo, packageKey string, callIndex int, errorLine int, filePath string) {
	// Read the file to analyze the error handling pattern
	content, err := os.ReadFile(filePath)
	if err != nil {
		c.debug("DEBUG: Failed to read file %s for error pattern analysis: %v\n", filePath, err)
		return
	}

	lines := strings.Split(string(content), "\n")
	if errorLine > len(lines) {
		c.debug("DEBUG: Error line %d exceeds file length %d\n", errorLine, len(lines))
		return
	}

	// Get the error handling line
	errorHandlingLine := strings.TrimSpace(lines[errorLine-1])
	c.debug("DEBUG: Analyzing error handling line: %s\n", errorHandlingLine)

	// Also check a few lines around the error line for context
	for i := max(0, errorLine-3); i < min(len(lines), errorLine+2); i++ {
		c.debug("DEBUG: Line %d: %s\n", i+1, strings.TrimSpace(lines[i]))
	}

	// Check if this is a simple errors.New call without meaningful context
	// Pattern: return errors.New(SomeCode, "generic message", err)
	// This should be either errors.AddContext(err, "key", value) or return err

	// Look for patterns that suggest no meaningful context is being added
	hasContext := c.detectMeaningfulContext(errorHandlingLine, lines, errorLine)

	// Determine the appropriate pattern
	call := &funcInfo.InternalPackageCalls[packageKey][callIndex]
	call.HasContext = hasContext

	if hasContext {
		// If there's meaningful context, suggest errors.AddContext
		call.ShouldUseAddContext = true
		call.ShouldReturnErr = false
	} else {
		// If no meaningful context, suggest simple return err
		call.ShouldReturnErr = true
		call.ShouldUseAddContext = false
	}

	c.debug("DEBUG: Pattern analysis - HasContext: %v, ShouldUseAddContext: %v, ShouldReturnErr: %v\n",
		call.HasContext, call.ShouldUseAddContext, call.ShouldReturnErr)
}

// detectMeaningfulContext checks if the error handling adds meaningful context
func (c *ErrorCodeChecker) detectMeaningfulContext(errorLine string, allLines []string, lineNum int) bool {
	// Look for patterns that indicate meaningful context is being added

	// Pattern 1: Check if the error message contains specific information
	// Look for variable interpolation or specific details
	if strings.Contains(errorLine, "%s") || strings.Contains(errorLine, "%v") || strings.Contains(errorLine, "%d") {
		c.debug("DEBUG: Found format specifiers in error message\n")
		return true
	}

	// Pattern 2: Check if there are AddContext calls nearby
	// Look in the surrounding lines for AddContext usage
	startLine := max(0, lineNum-3)
	endLine := min(len(allLines), lineNum+2)

	for i := startLine; i < endLine; i++ {
		line := strings.TrimSpace(allLines[i])
		if strings.Contains(line, ".AddContext(") {
			c.debug("DEBUG: Found AddContext call nearby at line %d\n", i+1)
			return true
		}
	}

	// Pattern 3: Check if the error message contains specific identifiers
	// Look for variable names or specific values that provide context
	// This is a simple heuristic - we look for patterns that suggest specific information
	contextIndicators := []string{
		"table", "database", "column", "row", "file", "path", "query", "user", "id", "name",
		"failed to", "unable to", "cannot", "invalid", "missing", "required",
	}

	errorLineLower := strings.ToLower(errorLine)
	for _, indicator := range contextIndicators {
		if strings.Contains(errorLineLower, indicator) {
			c.debug("DEBUG: Found context indicator '%s' in error message\n", indicator)
			return true
		}
	}

	// Pattern 4: Check if this is a generic wrapper
	// Generic patterns that suggest no meaningful context
	genericPatterns := []string{
		"\"failed to\"", "\"error occurred\"", "\"operation failed\"", "\"something went wrong\"",
		"\"internal error\"", "\"unexpected error\"", "\"error\"",
	}

	for _, pattern := range genericPatterns {
		if strings.Contains(errorLine, pattern) {
			c.debug("DEBUG: Found generic pattern '%s' - no meaningful context\n", pattern)
			return false
		}
	}

	// Default: assume no meaningful context if we can't determine otherwise
	c.debug("DEBUG: Could not determine context - defaulting to no meaningful context\n")
	return false
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

// Helper functions for min/max
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
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
