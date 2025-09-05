package main

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

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
		fmt.Printf("Error checking forbidden patterns: %v\n", err)
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

	report = append(report, "\n🚨 ERRORS.NEW USED FOR INTERNAL PACKAGE CALLS (should use ErrorCodes or better patterns):")

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

						// Enhanced reporting with pattern suggestions
						var suggestion string
						if call.ShouldUseAddContext {
							suggestion = " → Consider: errors.AddContext(err, \"key\", value)"
						} else if call.ShouldReturnErr {
							suggestion = " → Consider: return err"
						} else {
							suggestion = " → Use appropriate ErrorCode"
						}

						report = append(report, fmt.Sprintf("  ❌ INTERNAL_CALL: %s in %s:%d calls %s.%s at line %d, uses errors.New at line %d%s",
							info.Name, info.File, info.Line, call.Package, call.Function, call.Line, call.ErrorLine, suggestion))
					}
				}
			}
		}
	}

	return len(violations) == 0, report
}

// ReportEnhancedErrorPatterns reports enhanced error handling pattern suggestions
func (c *ErrorCodeChecker) ReportEnhancedErrorPatterns() (bool, []string) {
	var violations []string
	var report []string

	// Group functions by package
	packageGroups := make(map[string][]*FunctionInfo)
	for _, info := range c.functions {
		hasPatternIssues := false
		for _, calls := range info.InternalPackageCalls {
			for _, call := range calls {
				if call.ErrorLine > 0 && (call.ShouldUseAddContext || call.ShouldReturnErr) {
					hasPatternIssues = true
					break
				}
			}
			if hasPatternIssues {
				break
			}
		}

		if hasPatternIssues {
			packageGroups[info.Package] = append(packageGroups[info.Package], info)
		}
	}

	if len(packageGroups) == 0 {
		return true, []string{"✅ No enhanced error pattern issues found"}
	}

	// Sort packages for consistent output
	var packages []string
	for pkg := range packageGroups {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)

	report = append(report, "\n🔧 ENHANCED ERROR PATTERN SUGGESTIONS:")

	// Generate report grouped by package
	for _, pkg := range packages {
		report = append(report, fmt.Sprintf("\n📦 Package: %s", pkg))

		infos := packageGroups[pkg]
		sort.Slice(infos, func(i, j int) bool {
			return infos[i].Name < infos[j].Name
		})

		for _, info := range infos {
			for _, calls := range info.InternalPackageCalls {
				// Track suggestions for this function to avoid duplicates
				hasAddContextSuggestion := false
				hasReturnErrSuggestion := false
				var addContextCall, returnErrCall InternalCallInfo

				for _, call := range calls {
					if call.ErrorLine > 0 && (call.ShouldUseAddContext || call.ShouldReturnErr) {
						if call.ShouldUseAddContext && !hasAddContextSuggestion {
							hasAddContextSuggestion = true
							addContextCall = call
						} else if call.ShouldReturnErr && !hasReturnErrSuggestion && !hasAddContextSuggestion {
							hasReturnErrSuggestion = true
							returnErrCall = call
						}
					}
				}

				// Report the highest priority suggestion
				if hasAddContextSuggestion {
					violations = append(violations, fmt.Sprintf("%s:%s", info.File, info.Name))
					report = append(report, fmt.Sprintf("  🔧 PATTERN: %s in %s:%d calls %s.%s at line %d, uses errors.New at line %d",
						info.Name, info.File, info.Line, addContextCall.Package, addContextCall.Function, addContextCall.Line, addContextCall.ErrorLine))
					report = append(report, "    💡 Consider using errors.AddContext(err, \"key\", value) to add meaningful context")
				} else if hasReturnErrSuggestion {
					violations = append(violations, fmt.Sprintf("%s:%s", info.File, info.Name))
					report = append(report, fmt.Sprintf("  ↩️ PATTERN: %s in %s:%d calls %s.%s at line %d, uses errors.New at line %d",
						info.Name, info.File, info.Line, returnErrCall.Package, returnErrCall.Function, returnErrCall.Line, returnErrCall.ErrorLine))
					report = append(report, "    💡 Consider simply returning the error: return err")
				}
			}
		}
	}

	return len(violations) == 0, report
}
