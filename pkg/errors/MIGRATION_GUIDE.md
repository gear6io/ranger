# Error Handling Migration Guide

## Overview

This guide documents the migration from direct error type assertions to the `AsError()` utility function for consistent error handling throughout the codebase.

## Migration Pattern

### Old Pattern (Deprecated)

```go
// DON'T USE: Direct type assertion
if internalErr, ok := err.(errors.Error); ok {
    return internalErr
}
return errors.New(errors.CommonInternal, err.Error(), err)
```

### New Pattern (Recommended)

```go
// USE: AsError utility function
return errors.AsError(err)
```

## Why Migrate?

1. **Consistency**: AsError handles all error types uniformly
2. **Extensibility**: Supports InternalError interface for custom error types
3. **Maintainability**: Single function to update if error handling logic changes
4. **Context Preservation**: Automatically preserves rich context from enhanced errors

## How AsError Works

The `AsError()` function handles different error types:

1. **InternalError types** (like ParseError): Uses `Transform()` method to convert to internal format
2. **Existing errors.Error types**: Returns as-is (no conversion needed)
3. **Standard Go errors**: Wraps in generic internal error with CommonInternal code

## Migration Examples

### Example 1: Simple Error Conversion

```go
// OLD
func handleError(err error) error {
    if internalErr, ok := err.(errors.Error); ok {
        return internalErr
    }
    return errors.New(errors.CommonInternal, err.Error(), err)
}

// NEW
func handleError(err error) error {
    return errors.AsError(err)
}
```

### Example 2: Error with Context

```go
// OLD
func processData(data []byte) error {
    if err := validateData(data); err != nil {
        if internalErr, ok := err.(errors.Error); ok {
            return internalErr.AddContext("data_size", len(data))
        }
        return errors.New(errors.CommonInternal, err.Error(), err).
            AddContext("data_size", len(data))
    }
    return nil
}

// NEW
func processData(data []byte) error {
    if err := validateData(data); err != nil {
        return errors.AsError(err).AddContext("data_size", len(data))
    }
    return nil
}
```

### Example 3: Error Chain Handling

```go
// OLD
func complexOperation() error {
    if err := step1(); err != nil {
        if internalErr, ok := err.(errors.Error); ok {
            return internalErr.AddContext("step", "step1")
        }
        return errors.New(errors.CommonInternal, err.Error(), err).
            AddContext("step", "step1")
    }
    
    if err := step2(); err != nil {
        if internalErr, ok := err.(errors.Error); ok {
            return internalErr.AddContext("step", "step2")
        }
        return errors.New(errors.CommonInternal, err.Error(), err).
            AddContext("step", "step2")
    }
    
    return nil
}

// NEW
func complexOperation() error {
    if err := step1(); err != nil {
        return errors.AsError(err).AddContext("step", "step1")
    }
    
    if err := step2(); err != nil {
        return errors.AsError(err).AddContext("step", "step2")
    }
    
    return nil
}
```

## Special Cases

### ParseError Integration

ParseError implements the InternalError interface, so AsError automatically uses its Transform() method:

```go
parseErr := &ParseError{
    Code: ErrExpectedKeyword,
    Message: "expected semicolon",
    // ... other fields
}

// AsError automatically calls parseErr.Transform()
internalErr := errors.AsError(parseErr)
// Result includes position information, context, etc.
```

### Utility Functions

The existing utility functions continue to work with AsError:

```go
err := someOperation()
internalErr := errors.AsError(err)

// These functions work with the result
code := errors.GetCode(internalErr)
context := errors.GetContext(internalErr)
formatted := errors.FormatError(internalErr)
```

## Migration Checklist

When migrating code:

1. ✅ Replace `err.(errors.Error)` patterns with `errors.AsError(err)`
2. ✅ Remove manual error wrapping logic
3. ✅ Test that context and error information is preserved
4. ✅ Verify that error codes and messages remain consistent
5. ✅ Update any error handling tests

## Testing Migration

Use the test patterns in `parse_error_test.go` as examples:

```go
func TestMigrationPattern(t *testing.T) {
    var err error = // ... some error
    
    // Use AsError for consistent handling
    result := errors.AsError(err)
    
    // Verify result is proper internal error
    if !errors.IsRangerError(result) {
        t.Error("AsError should return internal error")
    }
    
    // Verify context preservation for enhanced errors
    if context := errors.GetContext(result); context != nil {
        // Check that important context is preserved
    }
}
```

## Benefits After Migration

1. **Reduced Code Duplication**: Single pattern for all error conversions
2. **Enhanced Error Context**: Automatic preservation of rich error information
3. **Future-Proof**: Easy to extend for new error types
4. **Consistent Logging**: All errors go through same conversion path
5. **Better Debugging**: Rich context preserved throughout error chains