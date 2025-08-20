package errors

import (
	"fmt"
	"runtime"
	"time"
)

// Error - simplified structure
type Error struct {
	Code      Code
	Message   string
	Cause     error
	Context   map[string]string
	Stack     []Frame
	Timestamp time.Time
}

// Frame represents a stack frame
type Frame struct {
	Function string
	File     string
	Line     int
}

// Core constructors - code is compulsory first argument
func New(code Code, message string) *Error {
	return &Error{
		Code:      code,
		Message:   message,
		Timestamp: time.Now(),
		Stack:     captureStackTrace(),
	}
}

func Newf(code Code, format string, args ...interface{}) *Error {
	return New(code, fmt.Sprintf(format, args...))
}

func Wrap(code Code, err error, message string) *Error {
	return &Error{
		Code:      code,
		Message:   message,
		Cause:     err,
		Timestamp: time.Now(),
		Stack:     captureStackTrace(),
	}
}

func Wrapf(code Code, err error, format string, args ...interface{}) *Error {
	return Wrap(code, err, fmt.Sprintf(format, args...))
}

// WithAdditional returns *Error directly
func WithAdditional(cause error, format string, args ...interface{}) *Error {
	if iceboxErr, ok := cause.(*Error); ok {
		newErr := &Error{
			Code:      iceboxErr.Code,
			Message:   iceboxErr.Message,
			Cause:     iceboxErr.Cause,
			Context:   make(map[string]string),
			Stack:     iceboxErr.Stack,
			Timestamp: iceboxErr.Timestamp,
		}

		// Copy existing context first
		if iceboxErr.Context != nil {
			for k, v := range iceboxErr.Context {
				newErr.Context[k] = v
			}
		}

		// Find the next available additional key index
		nextIndex := 0
		for {
			key := fmt.Sprintf("additional_%d", nextIndex)
			if _, exists := newErr.Context[key]; !exists {
				break
			}
			nextIndex++
		}

		// Add new additional context
		additionalKey := fmt.Sprintf("additional_%d", nextIndex)
		newErr.Context[additionalKey] = fmt.Sprintf(format, args...)

		return newErr
	}

	// For standard errors, create a new error with the additional context
	newErr := Wrap(CommonInternal, cause, fmt.Sprintf(format, args...))
	newErr.AddContext("additional_0", fmt.Sprintf(format, args...))
	return newErr
}

// Methods on *Error for chaining - only essential ones
func (e *Error) AddContext(key, value string) *Error {
	if e.Context == nil {
		e.Context = make(map[string]string)
	}
	e.Context[key] = value
	return e
}

func (e *Error) WithCause(err error) *Error {
	e.Cause = err
	return e
}

// Error methods
func (e *Error) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Cause)
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Cause
}

// Helper functions
func captureStackTrace() []Frame {
	var frames []Frame
	for i := 1; i < 10; i++ {
		pc, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		fn := runtime.FuncForPC(pc)
		frames = append(frames, Frame{
			Function: fn.Name(),
			File:     file,
			Line:     line,
		})
	}
	return frames
}
