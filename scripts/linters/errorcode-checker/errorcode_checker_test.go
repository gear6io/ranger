package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestErrorCodeChecker(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "errorcode-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create test files
	testFiles := map[string]string{
		"errors.go": `package test

import "github.com/gear6io/ranger/pkg/errors"

var (
	ErrTest1 = errors.MustNewCode("test.error1")
	ErrTest2 = errors.MustNewCode("test.error2")
	ErrTest3 = errors.MustNewCode("test.error3")
)
`,
		"usage.go": `package test

func testFunc() error {
	return errors.New(ErrTest1, "test error 1", nil)
}

func testFunc2() error {
	return errors.New(ErrTest2, "test error 2", nil)
}
`,
		"unused.go": `package test

func unusedFunc() {
	// This file doesn't use any ErrorCodes
}
`,
	}

	// Write test files
	for filename, content := range testFiles {
		filePath := filepath.Join(testDir, filename)
		if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write test file %s: %v", filename, err)
		}
	}

	// Test the checker
	checker := NewErrorCodeChecker(false)
	if err := checker.CheckDirectory(testDir, []string{}); err != nil {
		t.Fatalf("Failed to check directory: %v", err)
	}

	// Check results
	_, report := checker.Report()

	// Print report for debugging
	for _, line := range report {
		t.Log(line)
	}

	// Check specific ErrorCodes
	if info, exists := checker.errorCodes["ErrTest1"]; !exists || !info.Used {
		t.Error("ErrTest1 should be used")
	} else {
		t.Logf("ErrTest1 found at line %d", info.Line)
	}

	if info, exists := checker.errorCodes["ErrTest2"]; !exists || !info.Used {
		t.Error("ErrTest2 should be used")
	} else {
		t.Logf("ErrTest2 found at line %d", info.Line)
	}

	if info, exists := checker.errorCodes["ErrTest3"]; !exists || info.Used {
		t.Error("ErrTest3 should be unused")
	} else {
		t.Logf("ErrTest3 found at line %d", info.Line)
	}
}

func TestForbiddenPatterns(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "forbidden-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create test file with forbidden patterns
	testContent := `package test

import (
	"fmt"
	"errors"
)

func badFunc() error {
	return fmt.Errorf("bad error")
}

func badFunc2() error {
	return errors.New("bad error")
}

func badFunc3() error {
	return errors.Wrap(errors.New("bad"), "wrapped")
}
`
	filePath := filepath.Join(testDir, "bad.go")
	if err := os.WriteFile(filePath, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Test forbidden patterns checker
	checker := NewErrorCodeChecker(false)
	noViolations, report := checker.CheckForbiddenPatterns(testDir, []string{}, []string{`fmt\.Errorf`, `errors\.New\(`, `errors\.Wrap\(`, `errors\.Wrapf\(`})

	// Print report for debugging
	for _, line := range report {
		t.Log(line)
	}

	// Should find violations
	if noViolations {
		t.Error("Expected to find forbidden patterns")
	}

	// Should find at least 3 violations
	if len(report) < 3 {
		t.Errorf("Expected at least 3 violations, got %d", len(report))
	}
}

func TestExcludePaths(t *testing.T) {
	// Create a temporary test directory structure
	testDir, err := os.MkdirTemp("", "exclude-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create excluded directory
	excludedDir := filepath.Join(testDir, "excluded")
	if err := os.Mkdir(excludedDir, 0755); err != nil {
		t.Fatalf("Failed to create excluded dir: %v", err)
	}

	// Create test files
	testFiles := map[string]string{
		"errors.go": `package test

import "github.com/gear6io/ranger/pkg/errors"

var (
	ErrTest1 = errors.MustNewCode("test.error1")
)
`,
		"excluded/errors.go": `package excluded

import "github.com/gear6io/ranger/pkg/errors"

var (
	ErrExcluded = errors.MustNewCode("excluded.error")
)
`,
	}

	// Write test files
	for filename, content := range testFiles {
		filePath := filepath.Join(testDir, filename)
		if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
			t.Fatalf("Failed to write test file %s: %v", filename, err)
		}
	}

	// Test with exclude paths
	checker := NewErrorCodeChecker(false)
	excludePaths := []string{"excluded"}
	if err := checker.CheckDirectory(testDir, excludePaths); err != nil {
		t.Fatalf("Failed to check directory: %v", err)
	}

	// Check results
	_, report := checker.Report()

	// Print report for debugging
	for _, line := range report {
		t.Log(line)
	}

	// Should only find ErrTest1, not ErrExcluded
	if _, exists := checker.errorCodes["ErrExcluded"]; exists {
		t.Error("ErrExcluded should be excluded")
	}

	if _, exists := checker.errorCodes["ErrTest1"]; !exists {
		t.Error("ErrTest1 should be found")
	}
}

func TestInternalFunctionsUsingErrorsNew(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "internal-errors-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create test file with internal and external functions
	testContent := `package test

import (
	"github.com/gear6io/ranger/pkg/errors"
)

var (
	ErrTest1 = errors.MustNewCode("test.error1")
)

// Exported function - can use errors.New
func PublicFunction() error {
	return errors.New(ErrTest1, "public error", nil)
}

// Internal function - should not use errors.New
func internalFunction() error {
	return errors.New(ErrTest1, "internal error", nil)
}

// Another internal function with errors.New
func anotherInternal() error {
	return errors.New(ErrTest1, "another internal error", nil)
}

// Exported function with errors.New - this is acceptable
func AnotherPublic() error {
	return errors.New(ErrTest1, "another public error", nil)
}
`
	filePath := filepath.Join(testDir, "test.go")
	if err := os.WriteFile(filePath, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Test the checker
	checker := NewErrorCodeChecker(false)
	if err := checker.CheckDirectory(testDir, []string{}); err != nil {
		t.Fatalf("Failed to check directory: %v", err)
	}

	// Check internal functions report
	noViolations, report := checker.ReportInternalErrorsNewUsage()

	// Print report for debugging
	for _, line := range report {
		t.Log(line)
	}

	// Should find violations (internal functions using errors.New)
	if noViolations {
		t.Error("Expected to find internal functions using errors.New")
	}

	// Should find at least 2 internal functions using errors.New
	if len(report) < 3 { // Header + 2 internal functions
		t.Errorf("Expected at least 3 report lines, got %d", len(report))
	}

	// Check specific function tracking
	foundInternal := false
	foundAnotherInternal := false

	for _, funcInfo := range checker.functions {
		if funcInfo.Name == "internalFunction" && !funcInfo.Exported {
			foundInternal = true
			if !funcInfo.UsesErrorsNew {
				t.Error("internalFunction should be marked as using errors.New")
			}
			if len(funcInfo.ErrorsNewLines) == 0 {
				t.Error("internalFunction should have errors.New line numbers")
			}
		}
		if funcInfo.Name == "anotherInternal" && !funcInfo.Exported {
			foundAnotherInternal = true
			if !funcInfo.UsesErrorsNew {
				t.Error("anotherInternal should be marked as using errors.New")
			}
		}
	}

	if !foundInternal {
		t.Error("Should have found internalFunction")
	}
	if !foundAnotherInternal {
		t.Error("Should have found anotherInternal")
	}
}

func TestInternalPackageCallsUsingErrorsNew(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "internal-package-calls-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create test file with internal package calls
	testContent := `package test

import (
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/storage"
	"github.com/gear6io/ranger/client"
	"github.com/gear6io/ranger/server/query"
	"os"
)

var (
	ErrTest1 = errors.MustNewCode("test.error1")
)

// Function that calls internal package and uses errors.New
func testFunction() error {
	// Call to internal server package
	result, err := catalog.Parse("test")
	if err != nil {
		return errors.New(ErrTest1, "catalog parse failed", err)
	}

	// Call to external package - this should be allowed
	file, err := os.Open("test.txt")
	if err != nil {
		return errors.New(ErrTest1, "file open failed", err)
	}

	// Another internal server package call
	err = storage.Write(result)
	if err != nil {
		return errors.New(ErrTest1, "storage write failed", err)
	}

	// Call to internal pkg/errors package - this should also be flagged
	err = client.Connect("localhost")
	if err != nil {
		return errors.New(ErrTest1, "client connect failed", err)
	}

	return nil
}

// Function with only external package calls - should be allowed
func allowedFunction() error {
	file, err := os.ReadFile("test.txt")
	if err != nil {
		return errors.New(ErrTest1, "file read failed", err)
	}
	return nil
}

// Function that calls type methods from internal packages
func testTypeMethodCalls() error {
	// Create instances of internal types
	var catalogManager catalog.Manager
	var storageEngine storage.Engine
	var queryParser query.Parser

	// Call type methods - these should be detected as internal package calls
	result, err := catalogManager.LoadTable("test_table")
	if err != nil {
		return errors.New(ErrTest1, "catalog load failed", err)
	}

	err = storageEngine.WriteData(result)
	if err != nil {
		return errors.New(ErrTest1, "storage write failed", err)
	}

	parsed, err := queryParser.Parse("SELECT * FROM table")
	if err != nil {
		return errors.New(ErrTest1, "query parse failed", err)
	}

	return nil
}

// Function with chained method calls
func testChainedMethodCalls() error {
	var catalogManager catalog.Manager
	
	// Chained method calls should be detected
	table, err := catalogManager.GetNamespace("test").GetTable("users")
	if err != nil {
		return errors.New(ErrTest1, "chained method call failed", err)
	}

	return nil
}
`
	filePath := filepath.Join(testDir, "test.go")
	if err := os.WriteFile(filePath, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Test the checker
	checker := NewErrorCodeChecker(false)
	if err := checker.CheckDirectory(testDir, []string{}); err != nil {
		t.Fatalf("Failed to check directory: %v", err)
	}

	// Check internal package calls report
	noViolations, report := checker.ReportInternalPackageErrorsNewUsage()

	// Print report for debugging
	for _, line := range report {
		t.Log(line)
	}

	// Should find violations (internal package calls using errors.New)
	if noViolations {
		t.Error("Expected to find internal package calls using errors.New")
	}

	// Should find at least 6 internal package calls using errors.New
	// (3 from testFunction + 3 from testTypeMethodCalls + 1 from testChainedMethodCalls)
	if len(report) < 8 { // Header + 7 internal package calls
		t.Errorf("Expected at least 8 report lines, got %d", len(report))
	}

	// Check specific function tracking
	foundTestFunction := false
	foundTypeMethodFunction := false

	for _, funcInfo := range checker.functions {
		if funcInfo.Name == "testFunction" {
			foundTestFunction = true

			// Should have internal package calls
			if len(funcInfo.InternalPackageCalls) == 0 {
				t.Error("testFunction should have internal package calls")
			}

			// Check for catalog.Parse call
			catalogCalls, exists := funcInfo.InternalPackageCalls["catalog.Parse"]
			if !exists {
				t.Error("testFunction should have catalog.Parse call")
			} else if len(catalogCalls) == 0 {
				t.Error("catalog.Parse calls should not be empty")
			}

			// Check for storage.Write call
			storageCalls, exists := funcInfo.InternalPackageCalls["storage.Write"]
			if !exists {
				t.Error("testFunction should have storage.Write call")
			} else if len(storageCalls) == 0 {
				t.Error("storage.Write calls should not be empty")
			}

			// Check for client.Connect call
			clientCalls, exists := funcInfo.InternalPackageCalls["client.Connect"]
			if !exists {
				t.Error("testFunction should have client.Connect call")
			} else if len(clientCalls) == 0 {
				t.Error("client.Connect calls should not be empty")
			}
		}

		if funcInfo.Name == "testTypeMethodCalls" {
			foundTypeMethodFunction = true

			// Should have internal package calls for type methods
			if len(funcInfo.InternalPackageCalls) == 0 {
				t.Error("testTypeMethodCalls should have internal package calls")
			}

			// Check for catalog.Manager.LoadTable call
			_, exists := funcInfo.InternalPackageCalls["catalog.LoadTable"]
			if !exists {
				t.Error("testTypeMethodCalls should have catalog.LoadTable call")
			}

			// Check for storage.Engine.WriteData call
			_, exists = funcInfo.InternalPackageCalls["storage.WriteData"]
			if !exists {
				t.Error("testTypeMethodCalls should have storage.WriteData call")
			}

			// Check for query.Parser.Parse call
			_, exists = funcInfo.InternalPackageCalls["query.Parse"]
			if !exists {
				t.Error("testTypeMethodCalls should have query.Parse call")
			}
		}
	}

	if !foundTestFunction {
		t.Error("Should have found testFunction")
	}
	if !foundTypeMethodFunction {
		t.Error("Should have found testTypeMethodCalls")
	}
}

func TestEnhancedErrorPatternDetection(t *testing.T) {
	// Create a temporary test directory
	testDir, err := os.MkdirTemp("", "enhanced-pattern-test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(testDir)

	// Create test file with different error handling patterns
	testContent := `package test

import (
	"github.com/gear6io/ranger/pkg/errors"
	"github.com/gear6io/ranger/server/catalog"
	"github.com/gear6io/ranger/server/storage"
	"github.com/gear6io/ranger/server/query"
	"github.com/gear6io/ranger/server/metadata"
	"github.com/gear6io/ranger/server/filesystem"
	"github.com/gear6io/ranger/server/parser"
)

var (
	ErrTest1 = errors.MustNewCode("test.error1")
)

// Function with meaningful context - should suggest errors.AddContext
func testFunctionWithContext() error {
	result, err := catalog.Parse("test_table")
	if err != nil {
		return errors.New(ErrTest1, "failed to parse table %s", err)
	}
	return nil
}

// Function with AddContext nearby - should suggest errors.AddContext
func testFunctionWithAddContext() error {
	result, err := storage.Write("data")
	if err != nil {
		return errors.New(ErrTest1, "storage operation failed", err).
			AddContext("table", "users")
	}
	return nil
}

// Function with generic error - should suggest return err
func testFunctionGeneric() error {
	result, err := query.Execute("SELECT * FROM table")
	if err != nil {
		return errors.New(ErrTest1, "error occurred", err)
	}
	return nil
}

// Function with no meaningful context - should suggest return err
func testFunctionNoContext() error {
	result, err := metadata.Load("config")
	if err != nil {
		return errors.New(ErrTest1, "operation failed", err)
	}
	return nil
}

// Function with specific context indicators - should suggest errors.AddContext
func testFunctionSpecificContext() error {
	result, err := filesystem.ReadFile("config.json")
	if err != nil {
		return errors.New(ErrTest1, "failed to read database configuration", err)
	}
	return nil
}

// Function with format specifiers - should suggest errors.AddContext
func testFunctionWithFormat() error {
	result, err := parser.Parse("SELECT * FROM users")
	if err != nil {
		return errors.New(ErrTest1, "failed to parse query: %s", err)
	}
	return nil
}
`
	filePath := filepath.Join(testDir, "test.go")
	if err := os.WriteFile(filePath, []byte(testContent), 0644); err != nil {
		t.Fatalf("Failed to write test file: %v", err)
	}

	// Test the checker
	checker := NewErrorCodeChecker(false)
	if err := checker.CheckDirectory(testDir, []string{}); err != nil {
		t.Fatalf("Failed to check directory: %v", err)
	}

	// Check enhanced pattern report
	noViolations, report := checker.ReportEnhancedErrorPatterns()

	// Print report for debugging
	for _, line := range report {
		t.Log(line)
	}

	// Should find pattern suggestions
	if noViolations {
		t.Error("Expected to find enhanced error pattern suggestions")
	}

	// Should find at least 4 pattern suggestions
	// (testFunctionWithContext, testFunctionWithAddContext, testFunctionSpecificContext, testFunctionWithFormat should suggest AddContext)
	// (testFunctionGeneric, testFunctionNoContext should suggest return err)
	if len(report) < 8 { // Header + 6 pattern suggestions
		t.Errorf("Expected at least 8 report lines, got %d", len(report))
	}

	// Check specific function tracking
	foundWithContext := false
	foundGeneric := false
	foundSpecificContext := false
	foundWithFormat := false

	for _, funcInfo := range checker.functions {
		if funcInfo.Name == "testFunctionWithContext" {
			foundWithContext = true
			// Should suggest AddContext due to format specifier
			hasAddContextSuggestion := false
			for _, calls := range funcInfo.InternalPackageCalls {
				for _, call := range calls {
					if call.ShouldUseAddContext {
						hasAddContextSuggestion = true
						break
					}
				}
			}
			if !hasAddContextSuggestion {
				t.Error("testFunctionWithContext should suggest AddContext")
			}
		}

		if funcInfo.Name == "testFunctionGeneric" {
			foundGeneric = true
			// Should suggest return err due to generic message
			hasReturnErrSuggestion := false
			for _, calls := range funcInfo.InternalPackageCalls {
				for _, call := range calls {
					if call.ShouldReturnErr {
						hasReturnErrSuggestion = true
						break
					}
				}
			}
			if !hasReturnErrSuggestion {
				t.Error("testFunctionGeneric should suggest return err")
			}
		}

		if funcInfo.Name == "testFunctionSpecificContext" {
			foundSpecificContext = true
			// Should suggest AddContext due to specific context indicators
			hasAddContextSuggestion := false
			for _, calls := range funcInfo.InternalPackageCalls {
				for _, call := range calls {
					if call.ShouldUseAddContext {
						hasAddContextSuggestion = true
						break
					}
				}
			}
			if !hasAddContextSuggestion {
				t.Error("testFunctionSpecificContext should suggest AddContext")
			}
		}

		if funcInfo.Name == "testFunctionWithFormat" {
			foundWithFormat = true
			// Should suggest AddContext due to format specifier
			hasAddContextSuggestion := false
			for _, calls := range funcInfo.InternalPackageCalls {
				for _, call := range calls {
					if call.ShouldUseAddContext {
						hasAddContextSuggestion = true
						break
					}
				}
			}
			if !hasAddContextSuggestion {
				t.Error("testFunctionWithFormat should suggest AddContext")
			}
		}
	}

	if !foundWithContext {
		t.Error("Should have found testFunctionWithContext")
	}
	if !foundGeneric {
		t.Error("Should have found testFunctionGeneric")
	}
	if !foundSpecificContext {
		t.Error("Should have found testFunctionSpecificContext")
	}
	if !foundWithFormat {
		t.Error("Should have found testFunctionWithFormat")
	}
}

func TestContextDetectionLogic(t *testing.T) {
	checker := NewErrorCodeChecker(false)

	// Test cases for context detection
	testCases := []struct {
		name           string
		errorLine      string
		allLines       []string
		lineNum        int
		expectedResult bool
		description    string
	}{
		{
			name:           "Format specifier",
			errorLine:      `return errors.New(ErrTest, "failed to parse %s", err)`,
			allLines:       []string{},
			lineNum:        1,
			expectedResult: true,
			description:    "Should detect format specifiers as meaningful context",
		},
		{
			name:           "AddContext nearby",
			errorLine:      `return errors.New(ErrTest, "error", err)`,
			allLines:       []string{"", "", "    .AddContext(\"key\", value)"},
			lineNum:        3,
			expectedResult: true,
			description:    "Should detect AddContext calls nearby",
		},
		{
			name:           "Specific context indicators",
			errorLine:      `return errors.New(ErrTest, "failed to read database table", err)`,
			allLines:       []string{},
			lineNum:        1,
			expectedResult: true,
			description:    "Should detect specific context indicators",
		},
		{
			name:           "Generic error message",
			errorLine:      `return errors.New(ErrTest, "error occurred", err)`,
			allLines:       []string{},
			lineNum:        1,
			expectedResult: false,
			description:    "Should detect generic error messages as no context",
		},
		{
			name:           "Generic operation failed",
			errorLine:      `return errors.New(ErrTest, "operation failed", err)`,
			allLines:       []string{},
			lineNum:        1,
			expectedResult: false,
			description:    "Should detect generic operation failed as no context",
		},
		{
			name:           "Specific file operation",
			errorLine:      `return errors.New(ErrTest, "failed to read file", err)`,
			allLines:       []string{},
			lineNum:        1,
			expectedResult: true,
			description:    "Should detect specific file operation as meaningful context",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := checker.detectMeaningfulContext(tc.errorLine, tc.allLines, tc.lineNum)
			if result != tc.expectedResult {
				t.Errorf("%s: Expected %v, got %v. %s", tc.name, tc.expectedResult, result, tc.description)
			}
		})
	}
}
