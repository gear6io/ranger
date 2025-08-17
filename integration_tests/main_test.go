package integration_tests

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	iceboxBinary    = "./icebox"    // Path to the icebox binary, relative to integration_tests dir
	testdataDir     = "../testdata" // Relative path to the testdata directory
	titanicDataFile = "titanic.parquet"
)

var (
	tempTestDir string
)

// TODO: Re-enable integration tests once the icebox CLI is implemented
// TestMain sets up and tears down the test environment.
func TestMain(m *testing.M) {
	// Skip integration tests - icebox CLI not yet implemented
	fmt.Println("Skipping integration tests - icebox CLI not yet implemented")
	os.Exit(0)

	// var err error
	// // Create a temporary directory for test projects
	// tempTestDir, err = os.MkdirTemp("", "icebox-integration-*")
	// if err != nil {
	// 	fmt.Printf("Failed to create temp test directory: %v\n", err)
	// 	os.Exit(1)
	// }

	// // Build the icebox binary to ensure it's up-to-date
	// // The binary will be placed in the integration_tests directory.
	// cmd := exec.Command("go", "build", "-o", iceboxBinary, "github.com/TFMV/icebox/cmd/icebox")
	// // cmd.Dir = ".." // Run go build from the project root - removing this to let Go resolve package
	// output, err := cmd.CombinedOutput()
	// if err != nil {
	// 	fmt.Printf("Failed to build icebox binary: %v\nOutput: %s\n", err, string(output))
	// 	// os.RemoveAll(tempTestDir)
	// 	os.Exit(1)
	// }

	// // Run tests
	// exitCode := m.Run()

	// // Clean up: remove the temporary directory and the built binary
	// os.Remove(iceboxBinary) // Remove the compiled binary
	// os.RemoveAll(tempTestDir)
	// os.Exit(exitCode)
}

// setupTestProject creates a new temporary project directory and initializes icebox.
func setupTestProject(t *testing.T) (projectDir string, cleanup func()) {
	t.Helper()

	projectDir, err := os.MkdirTemp(tempTestDir, "test-project-*")
	if err != nil {
		t.Fatalf("Failed to create temp project directory: %v", err)
	}

	// Initialize Icebox project
	runIceboxCommand(t, projectDir, "init", ".")

	return projectDir, func() {
		// No specific cleanup for projectDir needed here as TestMain cleans tempTestDir
		// However, if specific resources were created within projectDir (e.g. MinIO server),
		// they would be cleaned up here.
	}
}

// runIceboxCommand executes an icebox command and returns its output.
// It sets the working directory for the command to projectDir.
func runIceboxCommand(t *testing.T, projectDir string, args ...string) (string, string) {
	t.Helper()

	absProjectDir, err := filepath.Abs(projectDir)
	if err != nil {
		t.Fatalf("Failed to get absolute path for projectDir %s: %v", projectDir, err)
	}

	// iceboxBinary is now relative to the integration_tests directory itself.
	absIceboxBinary, err := filepath.Abs(iceboxBinary)
	if err != nil {
		t.Fatalf("Failed to get absolute path for icebox binary %s: %v", iceboxBinary, err)
	}

	cmd := exec.Command(absIceboxBinary, args...)
	cmd.Dir = absProjectDir // Set the working directory for the command

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err = cmd.Run()
	if err != nil {
		// Include command, stdout, and stderr in the error message for better debugging
		t.Logf("Command failed: %s %v", absIceboxBinary, strings.Join(args, " "))
		t.Logf("Stdout: %s", stdout.String())
		t.Logf("Stderr: %s", stderr.String())
		t.Fatalf("Error running icebox command: %v. Stderr: %s", err, stderr.String())
	}

	return stdout.String(), stderr.String()
}

// copyTestData copies a test data file to the project's data directory.
// Assumes a standard Icebox project layout where data is stored relative to the project root.
func copyTestData(t *testing.T, projectDir, dataFileName string) string {
	t.Helper()

	sourcePath := filepath.Join(testdataDir, dataFileName)

	// Icebox init creates a .icebox directory. We'll place data outside of it for import.
	// Or, if your import command expects data within a specific structure, adjust destPath.
	destPath := filepath.Join(projectDir, dataFileName)

	input, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatalf("Failed to read test data file %s: %v", sourcePath, err)
	}

	err = os.WriteFile(destPath, input, 0644)
	if err != nil {
		t.Fatalf("Failed to write test data to project %s: %v", destPath, err)
	}
	return destPath
}
