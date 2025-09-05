package main

import (
	"go/ast"
	"go/token"
	"strings"
)

// SemanticAnalyzer provides semantic analysis for Go code
type SemanticAnalyzer struct {
	imports     map[string]string // alias -> full import path
	variables   map[string]string // var name -> type
	parameters  map[string]string // param name -> type
	currentFile *ast.File
	fileSet     *token.FileSet
}

// NewSemanticAnalyzer creates a new semantic analyzer
func NewSemanticAnalyzer(fileSet *token.FileSet) *SemanticAnalyzer {
	return &SemanticAnalyzer{
		imports:    make(map[string]string),
		variables:  make(map[string]string),
		parameters: make(map[string]string),
		fileSet:    fileSet,
	}
}

// analyzeFile performs semantic analysis on a Go file
func (sa *SemanticAnalyzer) analyzeFile(file *ast.File) {
	sa.currentFile = file
	sa.imports = make(map[string]string)
	sa.variables = make(map[string]string)
	sa.parameters = make(map[string]string)

	// Analyze imports
	sa.analyzeImports(file)

	// Analyze variable declarations and function parameters
	sa.analyzeDeclarations(file)
}

// analyzeImports analyzes import declarations
func (sa *SemanticAnalyzer) analyzeImports(file *ast.File) {
	for _, imp := range file.Imports {
		importPath := strings.Trim(imp.Path.Value, "\"")

		if imp.Name != nil {
			// Aliased import: import alias "path"
			sa.imports[imp.Name.Name] = importPath
		} else {
			// Standard import: import "path/to/package"
			parts := strings.Split(importPath, "/")
			if len(parts) > 0 {
				packageName := parts[len(parts)-1]
				sa.imports[packageName] = importPath
			}
		}
	}
}

// analyzeDeclarations analyzes variable declarations and function parameters
func (sa *SemanticAnalyzer) analyzeDeclarations(file *ast.File) {
	ast.Inspect(file, func(n ast.Node) bool {
		switch node := n.(type) {
		case *ast.GenDecl:
			if node.Tok == token.VAR {
				sa.analyzeVarDeclarations(node)
			}
		case *ast.AssignStmt:
			if node.Tok == token.DEFINE {
				sa.analyzeShortVarDeclarations(node)
			}
		case *ast.FuncDecl:
			if node.Type.Params != nil {
				sa.analyzeFunctionParams(node.Type.Params)
			}
		}
		return true
	})
}

// analyzeVarDeclarations analyzes var declarations
func (sa *SemanticAnalyzer) analyzeVarDeclarations(genDecl *ast.GenDecl) {
	for _, spec := range genDecl.Specs {
		if valueSpec, ok := spec.(*ast.ValueSpec); ok {
			for _, name := range valueSpec.Names {
				if valueSpec.Type != nil {
					sa.variables[name.Name] = sa.getTypeString(valueSpec.Type)
				}
			}
		}
	}
}

// analyzeShortVarDeclarations analyzes := declarations
func (sa *SemanticAnalyzer) analyzeShortVarDeclarations(assignStmt *ast.AssignStmt) {
	for i, lhs := range assignStmt.Lhs {
		if ident, ok := lhs.(*ast.Ident); ok {
			if i < len(assignStmt.Rhs) {
				sa.variables[ident.Name] = sa.inferTypeFromExpr(assignStmt.Rhs[i])
			}
		}
	}
}

// analyzeFunctionParams analyzes function parameters
func (sa *SemanticAnalyzer) analyzeFunctionParams(fieldList *ast.FieldList) {
	for _, field := range fieldList.List {
		typeStr := sa.getTypeString(field.Type)
		for _, name := range field.Names {
			sa.parameters[name.Name] = typeStr
		}
	}
}

// getTypeString converts an ast.Expr representing a type to a string
func (sa *SemanticAnalyzer) getTypeString(expr ast.Expr) string {
	switch t := expr.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.SelectorExpr:
		if x, ok := t.X.(*ast.Ident); ok {
			return x.Name + "." + t.Sel.Name
		}
	case *ast.StarExpr:
		return "*" + sa.getTypeString(t.X)
	case *ast.ArrayType:
		return "[]" + sa.getTypeString(t.Elt)
	}
	return "unknown"
}

// inferTypeFromExpr attempts to infer the type from an expression
func (sa *SemanticAnalyzer) inferTypeFromExpr(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.CallExpr:
		// For function calls, try to infer from the function name
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok {
			if x, ok := sel.X.(*ast.Ident); ok {
				return x.Name + "." + sel.Sel.Name + "()"
			}
		}
		if ident, ok := e.Fun.(*ast.Ident); ok {
			return ident.Name + "()"
		}
	case *ast.CompositeLit:
		if e.Type != nil {
			return sa.getTypeString(e.Type)
		}
	case *ast.Ident:
		return e.Name
	}
	return "unknown"
}

// isInternalPackageCall checks if a selector expression refers to an internal package call using semantic analysis
func (sa *SemanticAnalyzer) isInternalPackageCall(sel *ast.SelectorExpr) bool {
	if ident, ok := sel.X.(*ast.Ident); ok {
		// Check if it's a variable or parameter (method call)
		if _, isVar := sa.variables[ident.Name]; isVar {
			// This is a method call on a variable, check if the variable type is internal
			return sa.isInternalType(sa.variables[ident.Name])
		}
		if _, isParam := sa.parameters[ident.Name]; isParam {
			// This is a method call on a parameter, check if the parameter type is internal
			return sa.isInternalType(sa.parameters[ident.Name])
		}

		// Check if it's an actual package import
		if importPath, isImport := sa.imports[ident.Name]; isImport {
			return sa.isInternalImport(importPath)
		}

		// If not found in any scope, it's likely not a package call
		return false
	}
	return false
}

// isInternalImport checks if an import path is internal to the project
func (sa *SemanticAnalyzer) isInternalImport(importPath string) bool {
	// Exclude standard library and common external packages
	if sa.isStandardLibrary(importPath) || sa.isCommonExternalPackage(importPath) {
		return false
	}

	// Check if import is from the same module
	return strings.HasPrefix(importPath, "github.com/gear6io/ranger") ||
		strings.HasPrefix(importPath, "./") ||
		strings.HasPrefix(importPath, "../")
}

// isStandardLibrary checks if an import is from Go's standard library
func (sa *SemanticAnalyzer) isStandardLibrary(importPath string) bool {
	// Common standard library packages
	stdLibPackages := []string{
		"fmt", "strings", "strconv", "regexp", "time", "os", "io", "bufio",
		"bytes", "context", "encoding/json", "net/http", "database/sql",
		"log", "errors", "sort", "math", "crypto", "hash", "sync",
	}

	for _, pkg := range stdLibPackages {
		if importPath == pkg || strings.HasPrefix(importPath, pkg+"/") {
			return true
		}
	}

	// Standard library packages don't contain dots in their root
	return !strings.Contains(importPath, ".")
}

// isCommonExternalPackage checks if an import is from a common external package
func (sa *SemanticAnalyzer) isCommonExternalPackage(importPath string) bool {
	externalPrefixes := []string{
		"github.com/", "golang.org/", "google.golang.org/", "gopkg.in/",
		"go.uber.org/", "go.opentelemetry.io/",
	}

	for _, prefix := range externalPrefixes {
		if strings.HasPrefix(importPath, prefix) && !strings.HasPrefix(importPath, "github.com/gear6io/ranger") {
			return true
		}
	}

	return false
}

// isInternalType checks if a type is from an internal package using semantic analysis
func (sa *SemanticAnalyzer) isInternalType(typeName string) bool {
	// Only check if it's a qualified type from an internal package
	if strings.Contains(typeName, ".") {
		parts := strings.Split(typeName, ".")
		if len(parts) >= 2 {
			packageName := parts[0]
			if importPath, exists := sa.imports[packageName]; exists {
				return sa.isInternalImport(importPath)
			}
		}
	}

	// For unqualified types, we cannot determine if they're internal without
	// more sophisticated type analysis. Return false to avoid false positives.
	return false
}
