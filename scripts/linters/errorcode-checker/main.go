package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
)

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

	// Report enhanced error pattern suggestions if requested
	if config.CheckEnhancedPatterns {
		fmt.Println("🔍 Checking for enhanced error pattern suggestions...")
		noViolations, enhancedPatternReport := checker.ReportEnhancedErrorPatterns()

		for _, line := range enhancedPatternReport {
			fmt.Println(line)
		}
		fmt.Println()

		if !noViolations && config.ExitOnEnhancedPatterns {
			fmt.Println("❌ Found enhanced error pattern issues!")
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
		if config.ExitOnForbidden || config.ExitOnInternal || config.ExitOnInternalPackageCalls || config.ExitOnEnhancedPatterns {
			fmt.Println("🚨 Exiting due to linting violations (exit_on_*: true)")
			os.Exit(1)
		}
	}

	fmt.Println("✅ All checks completed successfully!")
	os.Exit(0)
}
