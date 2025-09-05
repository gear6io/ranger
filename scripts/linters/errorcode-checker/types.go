package main

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
	// Enhanced detection fields
	ShouldUseAddContext bool // Whether this should use errors.AddContext instead
	ShouldReturnErr     bool // Whether this should just return err instead
	HasContext          bool // Whether meaningful context is being added
}
