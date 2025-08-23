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

import "github.com/TFMV/icebox/pkg/errors"

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
	checker := NewErrorCodeChecker()
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
	checker := NewErrorCodeChecker()
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

import "github.com/TFMV/icebox/pkg/errors"

var (
	ErrTest1 = errors.MustNewCode("test.error1")
)
`,
		"excluded/errors.go": `package excluded

import "github.com/TFMV/icebox/pkg/errors"

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
	checker := NewErrorCodeChecker()
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
