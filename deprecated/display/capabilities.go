package display

import (
	"os"
	"runtime"
	"strings"

	"golang.org/x/term"
)

// TerminalCapabilities represents what the terminal supports
type TerminalCapabilities struct {
	SupportsColor   bool
	SupportsUnicode bool
	Width           int
	Height          int
	IsInteractive   bool
	IsPiped         bool
}

// DetectCapabilities automatically detects terminal capabilities
func DetectCapabilities() TerminalCapabilities {
	return TerminalCapabilities{
		SupportsColor:   detectColorSupport(),
		SupportsUnicode: detectUnicodeSupport(),
		Width:           getTerminalWidth(),
		Height:          getTerminalHeight(),
		IsInteractive:   term.IsTerminal(int(os.Stdin.Fd())),
		IsPiped:         isPiped(),
	}
}

// detectColorSupport checks if the terminal supports colors
func detectColorSupport() bool {
	// Check if we're in CI
	if isCI() {
		return false
	}

	// Check if stdout is a terminal
	if !term.IsTerminal(int(os.Stdout.Fd())) {
		return false
	}

	// Check environment variables
	if os.Getenv("NO_COLOR") != "" {
		return false
	}

	if os.Getenv("FORCE_COLOR") != "" {
		return true
	}

	// Check TERM environment variable
	termVar := os.Getenv("TERM")
	return termVar != "dumb"
}

// detectUnicodeSupport checks if the terminal supports Unicode
func detectUnicodeSupport() bool {
	// Windows Command Prompt has limited Unicode support
	if runtime.GOOS == "windows" {
		return false
	}

	// Check locale
	for _, env := range []string{"LC_ALL", "LC_CTYPE", "LANG"} {
		if val := os.Getenv(env); val != "" {
			if strings.Contains(strings.ToLower(val), "utf") {
				return true
			}
		}
	}

	return true
}

// isCI checks if we're running in a CI environment
func isCI() bool {
	ciVars := []string{"CI", "GITHUB_ACTIONS", "JENKINS_URL", "TRAVIS", "CIRCLECI"}
	for _, v := range ciVars {
		if os.Getenv(v) != "" {
			return true
		}
	}
	return false
}

// isPiped checks if output is being piped
func isPiped() bool {
	stat, _ := os.Stdout.Stat()
	return (stat.Mode() & os.ModeCharDevice) == 0
}

// getTerminalWidth gets the terminal width
func getTerminalWidth() int {
	width, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 80 // Default width
	}
	return width
}

// getTerminalHeight gets the terminal height
func getTerminalHeight() int {
	_, height, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil {
		return 24 // Default height
	}
	return height
}
